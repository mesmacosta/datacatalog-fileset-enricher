from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from google.cloud import datacatalog_v1
from google.api_core.exceptions import PermissionDenied
from google.protobuf.json_format import MessageToDict

from datacatalog_fileset_enricher.datacatalog_helper import DataCatalogHelper


@patch('google.cloud.datacatalog_v1.DataCatalogClient.__init__', lambda self, *args: None)
class DatacatalogHelperTestCase(TestCase):

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_tag')
    def test_delete_tag_should_not_raise_error(self, delete_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.delete_tag('entry_group_id', 'entry_id')

        delete_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_tag_template')
    def test_delete_tag_template_should_not_raise_error(self, delete_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.delete_tag_template()

        delete_tag_template.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_tag_template')
    def test_delete_tag_template_error_should_not_leak(self, delete_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')

        delete_tag_template.side_effect = Exception('error on delete tag template')

        datacatalog_helper.delete_tag_template()

        delete_tag_template.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.get_entry')
    def test_get_entry_should_not_raise_error(self, get_entry):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.get_entry('uscentral-1', 'test_entry_group', 'testr_entry')

        get_entry.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.search_catalog')
    def test_get_manually_created_fileset_entries_should_return_successfully(self, search_catalog):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = \
            'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_enricher_1/entries/entry_id_enricher_1'

        entry_2 = MockedObject()
        entry_2.name = 'fileset_entry'
        entry_2.relative_resource_name = \
            'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_enricher_1/entries/entry_id_enricher_2'

        entry_3 = MockedObject()
        entry_3.name = 'fileset_entry'
        entry_3.relative_resource_name = \
            'projects/uat-env-1/locations/us-central1/entryGroups/entry_group_enricher_2/entries/entry_id_enricher_3'

        search_catalog.return_value = [entry, entry_2, entry_3]

        results = datacatalog_helper.get_manually_created_fileset_entries()

        returned_location_1, returned_entry_1_entry_group, returned_entry_1_id = results[0]
        returned_location_2, returned_entry_2_entry_group, returned_entry_2_id = results[1]
        returned_location_3, returned_entry_3_entry_group, returned_entry_3_id = results[2]

        self.assertEqual('us-central1', returned_location_1)
        self.assertEqual('entry_group_enricher_1', returned_entry_1_entry_group)
        self.assertEqual('entry_id_enricher_1', returned_entry_1_id)
        self.assertEqual('us-central1', returned_location_2)
        self.assertEqual('entry_group_enricher_1', returned_entry_2_entry_group)
        self.assertEqual('entry_id_enricher_2', returned_entry_2_id)
        self.assertEqual('us-central1', returned_location_3)
        self.assertEqual('entry_group_enricher_2', returned_entry_3_entry_group)
        self.assertEqual('entry_id_enricher_3', returned_entry_3_id)

        search_catalog.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.get_tag_template')
    def test_get_tag_template_should_not_raise_error(self, get_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.get_fileset_enricher_tag_template('tag_template_name')

        get_tag_template.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag_template')
    def test_create_tag_template_should_create_all_fields(self, create_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.create_fileset_enricher_tag_template(
            'projects/my-project/locations/my-location/tagTemplates/tag_template_name')

        tag_template = create_tag_template.call_args_list[0][1]['tag_template']

        self.assertIsNotNone(tag_template)

        self.assertEqual('Number of files found', tag_template.fields['files'].display_name)
        self.assertEqual('Minimum file size found in megabytes',
                         tag_template.fields['min_file_size'].display_name)
        self.assertEqual('Maximum file size found in megabytes',
                         tag_template.fields['max_file_size'].display_name)
        self.assertEqual('Average file size found in megabytes',
                         tag_template.fields['avg_file_size'].display_name)
        self.assertEqual('Total file size found in megabytes',
                         tag_template.fields['total_file_size'].display_name)
        self.assertEqual('First time a file was created in the buckets',
                         tag_template.fields['first_created_date'].display_name)
        self.assertEqual('Last time a file was created in the buckets',
                         tag_template.fields['last_created_date'].display_name)
        self.assertEqual('Last time a file was updated in the buckets',
                         tag_template.fields['last_updated_date'].display_name)
        self.assertEqual('Number of files created on the same date',
                         tag_template.fields['created_files_by_day'].display_name)
        self.assertEqual('Number of files updated on the same date',
                         tag_template.fields['updated_files_by_day'].display_name)
        self.assertEqual('Prefix used to find the files',
                         tag_template.fields['prefix'].display_name)
        self.assertEqual('Buckets without this prefix were ignored',
                         tag_template.fields['bucket_prefix'].display_name)
        self.assertEqual('Number of buckets that matches the prefix',
                         tag_template.fields['buckets_found'].display_name)
        self.assertEqual('Number of files found on each bucket that matches the prefix',
                         tag_template.fields['files_by_bucket'].display_name)
        self.assertEqual('Number of files found by file type',
                         tag_template.fields['files_by_type'].display_name)
        self.assertEqual('Execution time when all stats were collected',
                         tag_template.fields['execution_time'].display_name)

        create_tag_template.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_no_files_found_should_not_raise_error(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        get_fileset_enricher_tag_template.return_value = tag_template
        list_tags.return_value = []

        stats = {
            'prefix': 'gs://my_bucket/*.csv',
            'count': 0,
            'files_by_bucket': '',
            'buckets_found': 0,
            'execution_time': pd.Timestamp.utcnow()
        }

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_not_called()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_files_found_should_not_raise_error(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        get_fileset_enricher_tag_template.return_value = tag_template
        list_tags.return_value = []

        stats = self.__create_full_stats_obj()

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_not_called()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_bucket_prefix_should_not_raise_error(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        get_fileset_enricher_tag_template.return_value = tag_template
        list_tags.return_value = []

        stats = self.__create_full_stats_obj()
        stats['bucket_prefix'] = 'my_bucket'

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_not_called()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_non_existent_template_should_not_raise_error(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'projects/my-project/locations/my-location/' \
                            'tagTemplates/fileset_template'

        create_fileset_enricher_tag_template.return_value = tag_template
        get_fileset_enricher_tag_template.side_effect = PermissionDenied('entry does not exist')
        list_tags.return_value = []

        stats = self.__create_full_stats_obj()

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_called_once()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_providing_tag_fields_should_filter_fields(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'projects/my-project/locations/my-location/' \
                            'tagTemplates/fileset_template'

        create_fileset_enricher_tag_template.return_value = tag_template
        get_fileset_enricher_tag_template.side_effect = PermissionDenied('entry does not exist')
        list_tags.return_value = []

        stats = self.__create_full_stats_obj()

        fields = ['files', 'prefix']

        datacatalog_helper.create_tag_from_stats(entry, stats, fields)

        tag = create_tag.call_args_list[0][1]['tag']

        fields_dict = MessageToDict(tag)['fields']

        self.assertTrue(set(fields).issubset(fields_dict))
        self.assertIn('execution_time', fields_dict)
        self.assertNotIn('files_by_bucket', fields_dict)
        self.assertNotIn('buckets_found', fields_dict)
        self.assertNotIn('bucket_prefix', fields_dict)
        self.assertNotIn('min_file_size', fields_dict)
        self.assertNotIn('max_file_size', fields_dict)
        self.assertNotIn('avg_file_size', fields_dict)
        self.assertNotIn('first_created_date', fields_dict)
        self.assertNotIn('last_created_date', fields_dict)
        self.assertNotIn('last_updated_date', fields_dict)
        self.assertNotIn('created_files_by_day', fields_dict)
        self.assertNotIn('updated_files_by_day', fields_dict)

        create_fileset_enricher_tag_template.assert_called_once()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_providing_tag_fields_with_key_error_should_leak_the_error(
        self, create_fileset_enricher_tag_template, get_fileset_enricher_tag_template, list_tags,
        create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'projects/my-project/locations/my-location/' \
                            'tagTemplates/fileset_template'

        create_fileset_enricher_tag_template.return_value = tag_template
        get_fileset_enricher_tag_template.side_effect = PermissionDenied('entry does not exist')
        list_tags.return_value = []

        stats = {
            'prefix': 'gs://my_bucket/*.csv',
            'count': 0,
            'files_by_bucket': '',
            'buckets_found': 0,
            'execution_time': pd.Timestamp.utcnow()
        }

        fields = ['files', 'prefix']

        datacatalog_helper.create_tag_from_stats(entry, stats, fields)

        tag = create_tag.call_args_list[0][1]['tag']

        fields_dict = MessageToDict(tag)['fields']

        self.assertTrue(set(fields).issubset(fields_dict))
        self.assertIn('execution_time', fields_dict)
        self.assertNotIn('files_by_bucket', fields_dict)
        self.assertNotIn('buckets_found', fields_dict)
        self.assertNotIn('bucket_prefix', fields_dict)
        self.assertNotIn('min_file_size', fields_dict)
        self.assertNotIn('max_file_size', fields_dict)
        self.assertNotIn('avg_file_size', fields_dict)
        self.assertNotIn('first_created_date', fields_dict)
        self.assertNotIn('last_created_date', fields_dict)
        self.assertNotIn('last_updated_date', fields_dict)
        self.assertNotIn('created_files_by_day', fields_dict)
        self.assertNotIn('updated_files_by_day', fields_dict)

        create_fileset_enricher_tag_template.assert_called_once()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.search_catalog')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry_group')
    def test_delete_entries_and_entry_groups_should_successfully_delete_them(
        self, delete_entry_group, delete_entry, search_catalog):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        entry_2 = MockedObject()
        entry_2.name = 'fileset_entry'
        entry_2.relative_resource_name = 'entry_group/entries/entry_id_2'

        entry_3 = MockedObject()
        entry_3.name = 'fileset_entry'
        entry_3.relative_resource_name = 'entry_group_2/entries/entry_id_3'

        search_catalog.return_value = [entry, entry_2, entry_3]

        datacatalog_helper.delete_entries_and_entry_groups()

        search_catalog.assert_called_once()
        self.assertEqual(3, delete_entry.call_count)
        self.assertEqual(2, delete_entry_group.call_count)

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.search_catalog')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry_group')
    def test_delete_entries_error_on_delete_entry_should_not_leak_error(
        self, delete_entry_group, delete_entry, search_catalog):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        entry_2 = MockedObject()
        entry_2.name = 'fileset_entry'
        entry_2.relative_resource_name = 'entry_group/entries/entry_id_2'

        entry_3 = MockedObject()
        entry_3.name = 'fileset_entry'
        entry_3.relative_resource_name = 'entry_group_2/entries/entry_id_3'

        search_catalog.return_value = [entry, entry_2, entry_3]

        delete_entry.side_effect = Exception('error on delete entry')

        datacatalog_helper.delete_entries_and_entry_groups()

        search_catalog.assert_called_once()
        self.assertEqual(3, delete_entry.call_count)
        delete_entry_group.assert_not_called()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.search_catalog')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.delete_entry_group')
    def test_delete_entries_error_on_delete_entry_group_should_not_leak_error(
        self, delete_entry_group, delete_entry, search_catalog):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        entry_2 = MockedObject()
        entry_2.name = 'fileset_entry'
        entry_2.relative_resource_name = 'entry_group/entries/entry_id_2'

        entry_3 = MockedObject()
        entry_3.name = 'fileset_entry'
        entry_3.relative_resource_name = 'entry_group_2/entries/entry_id_3'

        search_catalog.return_value = [entry, entry_2, entry_3]

        delete_entry_group.side_effect = Exception('error on delete entry')

        datacatalog_helper.delete_entries_and_entry_groups()

        search_catalog.assert_called_once()
        self.assertEqual(3, delete_entry.call_count)
        self.assertEqual(2, delete_entry_group.call_count)

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.update_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    def test_synchronize_entries_tags_should_update_tag_on_changes(self, list_tags, create_tag,
                                                                   update_tag):
        updated_tag = self.__make_fake_tag()
        current_tag = self.__make_fake_tag()
        current_tag.fields['test-double-field'].double_value = 2

        list_tags.return_value = [current_tag]

        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        datacatalog_helper.synchronize_entry_tags(entry, [updated_tag])

        create_tag.assert_not_called()
        update_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.update_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    def test_synchronize_entries_tags_should_not_update_tag_when_no_changes(
        self, list_tags, create_tag, update_tag):
        updated_tag = self.__make_fake_tag()
        current_tag = self.__make_fake_tag()

        list_tags.return_value = [current_tag]

        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        datacatalog_helper.synchronize_entry_tags(entry, [updated_tag])

        create_tag.assert_not_called()
        update_tag.assert_not_called()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.update_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    def test_synchronize_entries_tags_should_create_tag_when_tag_is_new(
        self, list_tags, create_tag, update_tag):
        updated_tag = self.__make_fake_tag()

        list_tags.return_value = []

        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        datacatalog_helper.synchronize_entry_tags(entry, [updated_tag])

        create_tag.assert_called_once()
        update_tag.assert_not_called()

    @patch('google.cloud.datacatalog_v1.DataCatalogClient.update_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1.DataCatalogClient.list_tags')
    def test_synchronize_entries_tags_should_do_nothing_when_no_tags_are_provided(
        self, list_tags, create_tag, update_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        entry.relative_resource_name = 'entry_group/entries/entry_id'

        datacatalog_helper.synchronize_entry_tags(entry, [])

        create_tag.assert_not_called()
        update_tag.assert_not_called()
        list_tags.assert_not_called()

    @classmethod
    def __create_full_stats_obj(cls):
        return {
            'prefix': 'gs://my_bucket*/*.csv',
            'count': 10,
            'files_by_bucket': 'my_bucket[count: 10]',
            'buckets_found': 1,
            'min_size': 1,
            'max_size': 1000,
            'avg_size': 500,
            'total_size': 1500,
            'min_created': pd.Timestamp.utcnow(),
            'max_created': pd.Timestamp.utcnow(),
            'max_updated': pd.Timestamp.utcnow(),
            'created_files_by_day': '10/06/2019 [count: 10]',
            'updated_files_by_day': '10/06/2019 [count: 10]',
            'files_by_type': 'csv [count: 1]',
            'execution_time': pd.Timestamp.utcnow()
        }

    @classmethod
    def __make_fake_tag(cls):
        tag = datacatalog_v1.types.Tag()
        tag.template = 'test-template'
        tag.fields['test-bool-field'].bool_value = True
        tag.fields['test-double-field'].double_value = 1
        tag.fields['test-string-field'].string_value = 'Test String Value'
        tag.fields['test-timestamp-field'].timestamp_value.FromJsonString(
            '2019-09-06T11:00:00-03:00')
        tag.fields['test-enum-field'].enum_value.display_name = 'Test ENUM Value'

        return tag


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
