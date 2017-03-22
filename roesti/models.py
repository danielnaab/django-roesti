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

    return obj


def make_hash(obj):
    return md5(pickle.dumps(freeze(obj))).hexdigest()


class HashedModelManager(models.Manager):
    def from_dict(self, item_dict):
        """
        Constructor that also initializes `content_hash`.
        """
        instance = self.model()
        instance.set_dict(item_dict)
        return instance

    @transaction.atomic
    def ensure(self, items):
        """
        Inserts each item in `items` to the database, if it doesn't already
        exist. Similar to an upsert operation. `items` may be dict-like
        mappings or model instances.

        The `content_hash` of each item will be (re)calculated on each item.

        Returns list of model instances.
        """
        return self._ensure_impl(items)

    def _ensure_impl(self, items):
        """
        Implmentation of `HashedModelManager.ensure`.
        In separate function so we can avoid nested transactions.
        """
        # Normalize `items` into a list of model instances where the primary
        # key is properly set.
        instances = []
        for item in items:
            if isinstance(item, collections.Mapping):
                instance = self.from_dict(item)
                instances.append(instance)
            elif isinstance(item, HashedModel):
                item.content_hash = item.get_content_hash()
                instances.append(item)
            else:
                raise ValueError('Item must be a Mapping or HashedModel')

        # Eliminate potential duplicate instances.
        instances = {
            instance.pk: instance
            for instance in instances
        }.values()

        # Ensure that foreign keys to other HashedModels exist.
        # First, get the foreign key fields we need to work with, aggregated
        # by table. eg, {table1: [field1, field2], table2: [field3]}
        table_references = collections.defaultdict(list)
        for field_name in self.model.hash_fields:
            field = self.model._meta.get_field(field_name)
            if isinstance(field, models.ForeignKey):
                table = field.rel.to
                if issubclass(table, HashedModel):
                    table_references[table].append(field.name)

        # Next, ensure that references for each table exist.
        for table, field_names in table_references.items():
            table.objects._ensure_impl(getattr(instance, field_name)
                                       for field_name in field_names
                                       for instance in instances)

        # Get the keys of the items that already exist in the database.
        all_pks = set(inst.pk for inst in instances)
        existing_pks = self.filter(pk__in=all_pks).values_list('pk', flat=True)

        # Insert instances that aren't in the db yet.
        # If everything already is in the db, skip the empty `bulk_create`.
        if len(all_pks) > len(existing_pks):
            self.bulk_create(instance
                             for instance in instances
                             if instance.pk not in existing_pks)

        return instances


class HashedModel(models.Model):
    objects = HashedModelManager()
    content_hash = models.CharField(primary_key=True, max_length=32)

    def save(self, *args, **kwargs):
        self.content_hash = self.get_content_hash()
        super(HashedModel, self).save(*args, **kwargs)

    def get_dict(self):
        return {
            field_name: getattr(self, field_name)
            for field_name in self.hash_fields
        }

    def get_content_hash(self):
        return make_hash(self.get_dict())

    def set_dict(self, item_dict):
        for field_name, value in item_dict.items():
            # If this is a dict-like value...
            if isinstance(value, collections.Mapping):
                field = self._meta.get_field(field_name)
                # ... And it corresponds to a reference to another HashedModel,
                # then try to instantiate it.
                if issubclass(field.rel.to, HashedModel):
                    value = field.rel.to.objects.from_dict(value)

            # Set this value on the instance.
            setattr(self, field_name, value)

        # Calculate the hash for this instance.
        self.content_hash = self.get_content_hash()

    def __str__(self):
        return self.content_hash

    class Meta:
        abstract = True
