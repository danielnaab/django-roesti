from django.db import models
from django.test import TestCase

from roesti.models import (
    HashedModel, HashedList, HashedListItemModel, make_hash)


def validate_hashes(test_case, instances):
    for instance in instances:
        test_case.assertEqual(instance.pk, instance.get_content_hash())


class TestModel(HashedModel):
    hash_fields = ['char_field_1', 'integer_field_1']

    char_field_1 = models.CharField(max_length=32)
    integer_field_1 = models.IntegerField()


class TestReferencesModel(HashedModel):
    hash_fields = ['test_model_1_id', 'test_model_2_id', 'integer_field_1']

    test_model_1 = models.ForeignKey(TestModel)
    test_model_2 = models.ForeignKey(TestModel, related_name='refs2')
    integer_field_1 = models.IntegerField()


class TestDeepReferencesModel(HashedModel):
    hash_fields = ['test_references_model_1_id', 'test_references_model_2_id',
                   'integer_field_1', 'char_field_1']

    test_references_model_1 = models.ForeignKey(TestReferencesModel)
    test_references_model_2 = models.ForeignKey(TestReferencesModel,
                                                related_name='refs2')
    integer_field_1 = models.IntegerField()
    char_field_1 = models.CharField(max_length=64)


class TestDeepReferencesDuplicateModel(HashedModel):
    hash_fields = ['test_references_model_1_id', 'test_references_model_2_id',
                   'test_model_1_id', 'test_model_2_id',
                   'integer_field_1', 'char_field_1']

    test_references_model_1 = models.ForeignKey(TestReferencesModel)
    test_references_model_2 = models.ForeignKey(TestReferencesModel,
                                                related_name='refs2_1')
    test_model_1 = models.ForeignKey(TestModel)
    test_model_2 = models.ForeignKey(TestModel, related_name='testrefs2')
    integer_field_1 = models.IntegerField()
    char_field_1 = models.CharField(max_length=64)


class HashedModelTestCase(TestCase):
    def test_model_create(self):
        data = [{
            'char_field_1': 'field 1 value 1',
            'integer_field_1': 1
        }, {
            'char_field_1': 'field 1 value 2',
            'integer_field_1': 2
        }, {
            'char_field_1': 'field 1 value 3',
            'integer_field_1': 3
        }]
        hashes = set(make_hash(item) for item in data)

        # One to query existing, one to insert all three rows, two for the
        # transaction.
        with self.assertNumQueries(4):
            instances = TestModel.objects.ensure(data)
            self.assertEqual(len(instances), 3)
            self.assertEqual(hashes, set(item.pk for item in instances))
            validate_hashes(self, instances)

        # One to query existing, no insert (already exist).
        # Two for the transaction.
        with self.assertNumQueries(3):
            instances = TestModel.objects.ensure(data)
            self.assertEqual(len(instances), 3)
            validate_hashes(self, instances)

        #
        # Do the same thing two more times, this time with model instances
        # rather than dictionaries.
        #
        # One to query existing, no insert (already exist).
        # Two for the transaction.
        with self.assertNumQueries(3):
            instances = TestModel.objects.ensure(TestModel(**item)
                                                 for item in data)
            self.assertEqual(len(instances), 3)
            self.assertEqual(hashes, set(item.pk for item in instances))
            validate_hashes(self, instances)

        # One to query existing, no insert (already exist).
        # Two for the transaction.
        with self.assertNumQueries(3):
            instances = TestModel.objects.ensure(TestModel(**item)
                                                 for item in data)
            self.assertEqual(len(instances), 3)
            self.assertEqual(hashes, set(item.pk for item in instances))
            validate_hashes(self, instances)


class TestReferencesModelTestCase(TestCase):
    def test_references_model_create_by_value(self):
        data = [{
            'test_model_1': {
                'char_field_1': 'field 1 value 1',
                'integer_field_1': 1
            },
            'test_model_2': {
                'char_field_1': 'field 1 value 2',
                'integer_field_1': 2
            },
            'integer_field_1': 3
        }]

        # 2 for transaction, 2 for TestModel, 2 for TestReferencesModel.
        with self.assertNumQueries(6):
            instances = TestReferencesModel.objects.ensure(data)
            self.assertEqual(len(instances), 1)
            validate_hashes(self, instances)

    def test_duplicate_insert(self):
        data = [{
            'test_model_1': {
                'char_field_1': 'field 1 value 1',
                'integer_field_1': 1
            },
            'test_model_2': {
                'char_field_1': 'field 1 value 2',
                'integer_field_1': 2
            },
            'integer_field_1': 3
        }, {
            'test_model_1': {
                'char_field_1': 'field 1 value 1',
                'integer_field_1': 1
            },
            'test_model_2': {
                'char_field_1': 'field 1 value 2',
                'integer_field_1': 2
            },
            'integer_field_1': 3
        }]

        # 2 for transaction
        # 2 each for TestModel and TestReferencesModel
        with self.assertNumQueries(6):
            instances = TestReferencesModel.objects.ensure(data)
            # Both of these instances have the same values, so there should
            # have only been one inserted.
            self.assertEqual(len(instances), 1)
            validate_hashes(self, instances)


class DeepHashedModelTestCase(TestCase):
    def test_deep_references_insert(self):
        data = [{
            'test_references_model_1': {
                'test_model_1': {
                    'char_field_1': 'field 1 value 1',
                    'integer_field_1': 1
                },
                'test_model_2': {
                    'char_field_1': 'field 1 value 2',
                    'integer_field_1': 2
                },
                'integer_field_1': 3
            },
            'test_references_model_2': {
                'test_model_1': {
                    'char_field_1': 'field 1 value 1',
                    'integer_field_1': 1
                },
                'test_model_2': {
                    'char_field_1': 'field 1 value 2',
                    'integer_field_1': 2
                },
                'integer_field_1': 3
            },
            'integer_field_1': 1,
            'char_field_1': 'string',
        }]

        # 2 for transaction
        # 2 for each of TestModel, TestReferencesModel, and
        # TestDeepReferencesModel
        with self.assertNumQueries(8):
            instances = TestDeepReferencesModel.objects.ensure(data)
            # Both of these instances have the same values, so there should
            # have only been one inserted.
            self.assertEqual(len(instances), 1)
            validate_hashes(self, instances)


class DeepHashDuplicateModelTestCase(TestCase):
    def test_deep_references_insert(self):
        data = [{
            'test_references_model_1': {
                'test_model_1': {
                    'char_field_1': 'field 1 value 1',
                    'integer_field_1': 1
                },
                'test_model_2': {
                    'char_field_1': 'field 1 value 2',
                    'integer_field_1': 2
                },
                'integer_field_1': 3
            },
            'test_references_model_2': {
                'test_model_1': {
                    'char_field_1': 'field 1 value 1',
                    'integer_field_1': 1
                },
                'test_model_2': {
                    'char_field_1': 'field 1 value 2',
                    'integer_field_1': 2
                },
                'integer_field_1': 3
            },
            'test_model_1': {
                'char_field_1': 'field 1 value 1',
                'integer_field_1': 1
            },
            'test_model_2': {
                'char_field_1': 'field 1 value 1',
                'integer_field_1': 1
            },
            'integer_field_1': 1,
            'char_field_1': 'string',
        }]

        # 2 for transaction
        # 2 for each of TestModel, TestReferencesModel, and
        # TestDeepReferencesDuplicateModel
        # 1 extra for the duplicated TestModel fields on the model instance.
        with self.assertNumQueries(9):
            instances = TestDeepReferencesDuplicateModel.objects.ensure(data)
            # Both of these instances have the same values, so there should
            # have only been one inserted.
            self.assertEqual(len(instances), 1)
            validate_hashes(self, instances)


class TestItem(HashedModel):
    hash_fields = ('text',)
    text = models.TextField(blank=True, null=True)


class TestListItem(HashedListItemModel):
    item = models.ForeignKey(TestItem)


class TestListReference(HashedModel):
    lst = models.ForeignKey(HashedList)


class TestListModel(TestCase):
    def test_list_items(self):
        items = [
            TestItem(text='Item %d' % index)
            for index in range(10)
        ]

        # 2 queries for transaction
        # 2 to verify list item existence and create them
        # 1 to check for existing list
        # 1 to create list
        # 1 to bulk insert list/item mapping
        # 1 extract for the `list_1.items.all()` query, below
        with self.assertNumQueries(8):
            list_1 = HashedList.objects.ensure_list(TestItem, items)
            list_1_items = list_1.items.all()
            self.assertEqual(len(list_1_items), 10)
            for index, instance in enumerate(list_1_items):
                self.assertEqual(instance.order, index + 1)

        # Ensure the existing list.
        # 2 queries for transaction, one to verify list items, one to check
        # for existing list.
        with self.assertNumQueries(4):
            list_2 = HashedList.objects.ensure_list(TestItem, items)
            self.assertEqual(list_1.pk, list_2.pk)


class TestBackrefModel(HashedModel):
    hash_fields = ('text',)
    text = models.TextField()


class TestBackrefReference(HashedModel):
    hash_fields = ('ref_text',)
    ref_text = models.TextField()
    backref = models.ForeignKey(TestBackrefModel, related_name='items')


class TestReverseReferences(TestCase):
    def test_reverse_references(self):
        with self.assertNumQueries(6):
            instances = TestBackrefModel.objects.ensure([{
                # `items` is a related field set. Note that this is unordered.
                'items': [{
                    'ref_text': 'Item %d' % index
                } for index in range(10)]
            }])
        instances = list(instances)
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0].items.all()), 10)


class TestOrderedList(HashedModel):
    hash_fields = ('name', 'items')
    name = models.TextField()


class TestItemDetails(HashedModel):
    hash_fields = ('text',)
    text = models.TextField()


class TestOrderedListItem(HashedModel):
    hash_fields = ('lst_id', 'order', 'details',)
    lst = models.ForeignKey(TestOrderedList, related_name='items')
    order = models.IntegerField()
    details = models.ForeignKey(TestItemDetails)


class OrderedListTestCase(TestCase):
    def test_reverse_references(self):
        with self.assertNumQueries(8):
            instances = TestOrderedList.objects.ensure([{
                'name': 'My list',
                'items': [{
                    'order': index,
                    'details': {
                        'text': 'Item %d' % index
                    }
                } for index in range(10)]
            }])
        instances = list(instances)
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0].items.all()), 10)

        # Re-insert the same list.
        with self.assertNumQueries(5):
            instances = TestOrderedList.objects.ensure([{
                'name': 'My list',
                'items': [{
                    'order': index,
                    'details': {
                        'text': 'Item %d' % index
                    }
                } for index in range(10)]
            }])
        instances = list(instances)
        self.assertEqual(len(instances), 1)
        self.assertEqual(len(instances[0].items.all()), 10)

        # Ensure that nothing extra was added to the DB.
        self.assertEqual(TestOrderedList.objects.count(), 1)
        self.assertEqual(TestItemDetails.objects.count(), 10)
        self.assertEqual(TestOrderedListItem.objects.count(), 10)

        # Insert multiple lists
        with self.assertNumQueries(8):
            instances = TestOrderedList.objects.ensure([{
                'name': 'My list 1',
                'items': [{
                    'order': index,
                    'details': {
                        'text': '1 Item %d' % index
                    }
                } for index in range(10)]
            }, {
                'name': 'My list 2',
                'items': [{
                    'order': index,
                    'details': {
                        'text': '2 Item %d' % index
                    }
                } for index in range(10)]
            }, {
                'name': 'My list 3',
                'items': [{
                    'order': index,
                    'details': {
                        'text': '3 Item %d' % index
                    }
                } for index in range(10)]
            }])
        instances = list(instances)
        self.assertEqual(len(instances), 3)
        self.assertEqual(len(instances[0].items.all()), 10)
        self.assertEqual(len(instances[1].items.all()), 10)
        self.assertEqual(len(instances[2].items.all()), 10)

        # Ensure that nothing extra was added to the DB.
        self.assertEqual(TestOrderedList.objects.count(), 4)
        self.assertEqual(TestItemDetails.objects.count(), 40)
        self.assertEqual(TestOrderedListItem.objects.count(), 40)

        # Check content of items.
        self.assertEqual(
            set(TestOrderedList.objects.all().values_list('name', flat=True)),
            set(['My list', 'My list 1', 'My list 2', 'My list 3'])
        )
        self.assertEqual(
            set(TestOrderedListItem.objects.all().values_list('order', flat=True)),
            set(range(10))
        )
        self.assertEqual(
            set(TestItemDetails.objects.all().values_list('text', flat=True)),
            set(
                frmt % index
                for index in range(10)
                for frmt in ['Item %d', '1 Item %d', '2 Item %d', '3 Item %d']
            )
        )
