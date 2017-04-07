import collections
from hashlib import md5
import pickle

from django.db import models, transaction


def freeze(obj):
    # If this is dict-like, return a sorted tuple.
    if hasattr(obj, 'items') and hasattr(obj.items, '__call__'):
        return tuple(sorted((key, freeze(value))
                            for key, value in obj.items()))

    # If a list, return a tuple.
    if isinstance(obj, list):
        return tuple(freeze(value) for value in obj)

    # If a set, return a sorted tuple.
    if isinstance(obj, set):
        return tuple(sorted(obj))

    return obj


def make_hash(obj):
    return md5(pickle.dumps(freeze(obj))).hexdigest()


class HashedModelManager(models.Manager):
    def from_dict(self, item_dict):
        """
        Constructor that also initializes `content_hash`.
        """
        instance = self.model()
        related = instance.set_dict(item_dict)
        return instance, related

    @transaction.atomic
    def ensure(self, items):
        """
        Inserts each item in `items` to the database, if it doesn't already
        exist. Similar to an upsert operation. `items` may be dict-like
        mappings or model instances.

        The `content_hash` of each item will be (re)calculated on each item.

        Returns list of model instances.
        """
        return list(self._ensure_impl(items))

    def _ensure_impl(self, items):
        """
        Implmentation of `HashedModelManager.ensure`.
        In separate function so we can avoid nested transactions.
        """
        # Normalize `items` into a list of model instances where the primary
        # key is properly set.
        instances = []
        related_mapping = collections.defaultdict(set)
        for item in items:
            if isinstance(item, collections.Mapping):
                instance, related_models = self.from_dict(item)
                for key, related_instances in related_models.items():
                    related_mapping[key].update(related_instances)
                instances.append(instance)
            elif isinstance(item, HashedModel):
                if not item.content_hash:
                    item.content_hash = item.get_content_hash()
                instances.append(item)
            else:
                raise ValueError('Item must be a Mapping or HashedModel')

        instances = self._do_insert(self.model, instances)

        # Now, insert all the instances that have back-references to this one.
        for (model, field_name), related_instances in related_mapping.items():
            self._do_insert(model, related_instances, skip_ensure=[self.model])

        return instances

    def _do_insert(self, InsertModel, instances, skip_ensure=[]):
        # Eliminate potential duplicate instances.
        instances = {
            instance.pk: instance
            for instance in instances
        }.values()

        # Ensure that foreign keys to other HashedModels exist.
        # First, get the foreign key fields we need to work with, aggregated
        # by table. eg, {table1: [field1, field2], table2: [field3]}
        table_references = collections.defaultdict(list)
        for field_name in InsertModel.hash_fields:
            field = InsertModel._meta.get_field(field_name)
            if isinstance(field, models.ForeignKey):
                table = field.rel.to
                if issubclass(table, HashedModel):
                    table_references[table].append(field.name)

        # Next, ensure that references for each table exist.
        for table, field_names in table_references.items():
            if table in skip_ensure:
                continue
            table.objects._ensure_impl(getattr(instance, field_name)
                                       for field_name in field_names
                                       for instance in instances)

        # Get the keys of the items that already exist in the database.
        all_pks = set(inst.pk for inst in instances)
        existing_pks = InsertModel.objects.filter(
            pk__in=all_pks).values_list('pk', flat=True)

        # Insert instances that aren't in the db yet.
        # If everything already is in the db, skip the empty `bulk_create`.
        if len(all_pks) > len(existing_pks):
            InsertModel.objects.bulk_create(instance
                                            for instance in instances
                                            if instance.pk not in existing_pks)

        return instances


class HashField(models.CharField):
    def __init__(self, **kwargs):
        defaults = {
            'max_length': 32
        }
        defaults.update(kwargs)
        super(HashField, self).__init__(**defaults)


class HashedModel(models.Model):
    objects = HashedModelManager()
    content_hash = HashField(primary_key=True)

    def save(self, *args, **kwargs):
        self.content_hash = self.get_content_hash()
        super(HashedModel, self).save(*args, **kwargs)

    def _get_hash_field(self, field_name, reverse_relations):
        value = getattr(self, field_name)

        # If this is a related field manager, get the fields as an unordered
        # set of the instance IDs (hashes).
        if issubclass(value.__class__, models.Manager):
            for (model, field), instances in reverse_relations.items():
                if value.model == model and field == value.field.get_attname():
                    value = set(instance.pk for instance in instances)
                    break

        return value

    def _get_hash_field_dict(self, reverse_relations):
        return {
            field_name: self._get_hash_field(field_name, reverse_relations)
            for field_name in self.hash_fields
        }

    def get_content_hash(self, reverse_relations={}):
        return make_hash(self._get_hash_field_dict(reverse_relations))

    def _accumulate_dict(self, target, source):
        if not source:
            return
        for key, value in source.items():
            target[key].update(value)

    def set_dict(self, item_dict):
        # Will accumulate ManyToMany relations here, in the form:
        # {ModelClass: [instance1, instance2, ...]}
        reverse_relations = collections.defaultdict(set)

        for field_name, value in item_dict.items():
            field = self._meta.get_field(field_name)

            # If this is a dict-like value...
            if isinstance(value, collections.Mapping):
                # ... And it corresponds to a reference to another HashedModel,
                # then try to instantiate it.
                if issubclass(field.rel.to, HashedModel):
                    value, related = field.rel.to.objects.from_dict(value)
                    self._accumulate_dict(reverse_relations, related)

            # If this is a non-string iterable...
            elif isinstance(value, collections.Iterable) and not isinstance(
                    value, str):
                # ... And it corresponds to a reverse relation, we will try to
                # create the objects and accumulate them.
                if type(field) == models.ManyToOneRel:
                    RelatedModel = field.related_model

                    # For each reverse relation, create the instance and
                    # accumulate in `reverse_relations`.
                    for item in value:
                        instance, related = RelatedModel.objects.from_dict(item)
                        key = (RelatedModel, field.remote_field.get_attname())
                        reverse_relations[key].add(instance)
                        self._accumulate_dict(reverse_relations, related)

                    # Don't set related fields on the instance itself.
                    value = None

            # Set this value on the instance.
            if value is not None:
                setattr(self, field_name, value)

        # Calculate the hash for this instance.
        self.content_hash = self.get_content_hash(reverse_relations)

        # Set all the back-references to this instance.
        for (model, field_name), instances in reverse_relations.items():
            # If this set of relations doesn't refer to this model, skip.
            field = model._meta.get_field(field_name)
            if self.__class__ != field.rel.to:
                continue

            for instance in instances:
                setattr(instance, field_name, self.content_hash)

        return reverse_relations

    def __str__(self):
        return self.content_hash

    class Meta:
        abstract = True


class HashedListModelManager(models.Manager):
    def get_list(self, list_hash):
        return self.filter(list_hash=list_hash)

    @transaction.atomic
    def ensure_list(self, ItemModel, items):
        # Ensure the list items exist.
        item_instances = ItemModel.objects._ensure_impl(items)

        # Calculate the hash of this list as the hash of the list of its keys.
        list_hash = make_hash([item.pk for item in item_instances])

        # If this list already exists, return it.
        lst = self.filter(pk=list_hash)
        if len(lst) > 0:
            return lst[0]

        # Create the HashedList model instance...
        lst_instance = self.create(pk=list_hash)

        # ... and assign each of these list items to it with back references.
        ListItemModel = self.model.items.field.model
        items = ListItemModel.objects.ensure_items(list_hash, item_instances)

        return lst_instance


class HashedList(models.Model):
    objects = HashedListModelManager()
    list_hash = HashField(primary_key=True)

    def __str__(self):
        return self.list_hash


class HashedListItemModelManager(HashedModelManager):
    def get_list(self, list_hash):
        return self.filter(list_hash=list_hash)

    def ensure_items(self, list_hash, items):
        return self.bulk_create([
            self.model(
                list_hash_id=list_hash,
                order=order,
                item=item
            )
            for order, item in enumerate(items, 1)
        ])


class HashedListItemModel(models.Model):
    """
    Implements a model that groups items in a list, with `list_hash` equal to a
    hash of the list's references. The concrete class must define `item` and
    `list_hash` fields.
    """
    objects = HashedListItemModelManager()

    list_hash = models.ForeignKey(HashedList, related_name='items')
    order = models.PositiveIntegerField()

    # Set this field in the concrete class.
    #item = models.ForeignKey(ListItemModel)

    class Meta:
        abstract = True
        ordering = ('order',)
