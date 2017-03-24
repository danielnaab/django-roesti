from django.db import models
from django.test import TestCase

from roesti.models import HashedModel, make_hash


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
    test_model_2 = models.ForeignKey(TestModel)
    integer_field_1 = models.IntegerField()


class TestDeepReferencesModel(HashedModel):
    hash_fields = ['test_references_model_1_id', 'test_references_model_2_id',
                   'integer_field_1', 'char_field_1']

    test_references_model_1 = models.ForeignKey(TestReferencesModel)
    test_references_model_2 = models.ForeignKey(TestReferencesModel)
    integer_field_1 = models.IntegerField()
    char_field_1 = models.CharField(max_length=64)


class TestDeepReferencesDuplicateModel(HashedModel):
    hash_fields = ['test_references_model_1_id', 'test_references_model_2_id',
                   'test_model_1_id', 'test_model_2_id',
                   'integer_field_1', 'char_field_1']

    test_references_model_1 = models.ForeignKey(TestReferencesModel)
    test_references_model_2 = models.ForeignKey(TestReferencesModel)
    test_model_1 = models.ForeignKey(TestModel)
    test_model_2 = models.ForeignKey(TestModel)
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
