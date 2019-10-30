from unittest import TestCase
from unittest.mock import patch

import pandas as pd
from google.api_core.exceptions import PermissionDenied
from google.protobuf.json_format import MessageToDict

from datacatalog_fileset_enricher.datacatalog_helper import DataCatalogHelper


@patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.__init__', lambda self, *args: None)
class DatacatalogHelperTestCase(TestCase):

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.delete_tag')
    def test_delete_tag_should_not_raise_error(self, delete_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.delete_tag('entry_group_id', 'entry_id')

        delete_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.delete_tag_template')
    def test_delete_tag_template_should_not_raise_error(self, delete_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.delete_tag_template()

        delete_tag_template.assert_called_once()

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag_template')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.location_path')
    def test_create_tag_template_should_create_all_fields(self, location_path,
                                                          create_tag_template):
        datacatalog_helper = DataCatalogHelper('test_project')
        datacatalog_helper.create_fileset_enricher_tag_template()

        tag_template = create_tag_template.call_args_list[0][1]['tag_template']

        self.assertIsNotNone(tag_template)

        self.assertEqual('Number of files found', tag_template.fields['files'].display_name)
        self.assertEqual('Minimum file size found in bytes',
                         tag_template.fields['min_file_size'].display_name)
        self.assertEqual('Maximum file size found in bytes',
                         tag_template.fields['max_file_size'].display_name)
        self.assertEqual('Average file size found in bytes',
                         tag_template.fields['avg_file_size'].display_name)
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
        self.assertEqual('Execution time when all stats were collected',
                         tag_template.fields['execution_time'].display_name)

        create_tag_template.assert_called_once()
        location_path.assert_called_once()

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_no_files_found_should_not_raise_error(self,
                                                                   create_fileset_enricher_tag_template,
                                                                   get_fileset_enricher_tag_template,
                                                                   list_tags,
                                                                   create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        get_fileset_enricher_tag_template.return_value = tag_template
        list_tags.return_value = []

        stats = {'prefix': 'gs://my_bucket/*.csv',
                 'count': 0,
                 'files_by_bucket': '',
                 'buckets_found': 0,
                 'execution_time': pd.Timestamp.utcnow()}

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_not_called()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_files_found_should_not_raise_error(self,
                                                                create_fileset_enricher_tag_template,
                                                                get_fileset_enricher_tag_template,
                                                                list_tags,
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

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_with_bucket_prefix_should_not_raise_error(self,
                                                                  create_fileset_enricher_tag_template,
                                                                  get_fileset_enricher_tag_template,
                                                                  list_tags,
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

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_non_existent_template_should_not_raise_error(self,
                                                                     create_fileset_enricher_tag_template,
                                                                     get_fileset_enricher_tag_template,
                                                                     list_tags,
                                                                     create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        create_fileset_enricher_tag_template.return_value = tag_template
        get_fileset_enricher_tag_template.side_effect = PermissionDenied('entry does not exist')
        list_tags.return_value = []

        stats = self.__create_full_stats_obj()

        datacatalog_helper.create_tag_from_stats(entry, stats)

        create_fileset_enricher_tag_template.assert_called_once()
        get_fileset_enricher_tag_template.assert_called_once()
        list_tags.assert_called_once()
        create_tag.assert_called_once()

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_providing_tag_fields_should_filter_fields(self,
                                                                  create_fileset_enricher_tag_template,
                                                                  get_fileset_enricher_tag_template,
                                                                  list_tags,
                                                                  create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

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

    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.create_tag')
    @patch('google.cloud.datacatalog_v1beta1.DataCatalogClient.list_tags')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'get_fileset_enricher_tag_template')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.'
           'create_fileset_enricher_tag_template')
    def test_create_tag_providing_tag_fields_with_key_error_should_leak_the_error(self,
                                                                                  create_fileset_enricher_tag_template,
                                                                                  get_fileset_enricher_tag_template,
                                                                                  list_tags,
                                                                                  create_tag):
        datacatalog_helper = DataCatalogHelper('test_project')
        entry = MockedObject()
        entry.name = 'fileset_entry'
        tag_template = MockedObject()
        tag_template.name = 'fileset_template'

        create_fileset_enricher_tag_template.return_value = tag_template
        get_fileset_enricher_tag_template.side_effect = PermissionDenied('entry does not exist')
        list_tags.return_value = []

        stats = {'prefix': 'gs://my_bucket/*.csv',
                 'count': 0,
                 'files_by_bucket': '',
                 'buckets_found': 0,
                 'execution_time': pd.Timestamp.utcnow()}

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

    @classmethod
    def __create_full_stats_obj(cls):
        return {'prefix': 'gs://my_bucket*/*.csv',
                'count': 10,
                'files_by_bucket': 'my_bucket[count: 10]',
                'buckets_found': 1,
                'min_size': 1,
                'max_size': 1000,
                'avg_size': 500,
                'min_created': pd.Timestamp.utcnow(),
                'max_created': pd.Timestamp.utcnow(),
                'max_updated': pd.Timestamp.utcnow(),
                'created_files_by_day': '10/06/2019[count: 10]',
                'updated_files_by_day': '10/06/2019[count: 10]',
                'execution_time': pd.Timestamp.utcnow()}


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
