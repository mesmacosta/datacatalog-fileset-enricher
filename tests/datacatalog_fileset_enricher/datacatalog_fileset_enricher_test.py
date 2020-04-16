import pandas as pd

from unittest import TestCase
from unittest.mock import patch

from google.cloud import datacatalog_v1

from datacatalog_fileset_enricher.datacatalog_fileset_enricher import DatacatalogFilesetEnricher


@patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.__init__',
       lambda self, *args: None)
@patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.__init__',
       lambda self, *args: None)
class DatacatalogFilesetEnricherTestCase(TestCase):

    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.delete_tag_template')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.delete_entries_and_entry_groups'
    )
    def test_clean_up_all_should_call_the_right_clean_up_methods(self,
                                                                 delete_entries_and_entry_groups,
                                                                 delete_tag_template):
        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.clean_up_all()

        delete_entries_and_entry_groups.assert_called_once()
        delete_tag_template.assert_called_once()

    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.delete_tag_template')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.delete_entries_and_entry_groups'
    )
    def test_clean_up_fileset_template_and_tags_should_call_the_right_clean_up_methods(
        self, delete_entries_and_entry_groups, delete_tag_template):
        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.clean_up_fileset_template_and_tags()

        delete_entries_and_entry_groups.assert_not_called()
        delete_tag_template.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.create_tag_from_stats')
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_stats_summarizer.GCStorageStatsSummarizer.create_stats_from_dataframe'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_multiple_buckets'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_single_bucket'
    )
    @patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.parse_gcs_file_patterns')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_entry')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_manually_created_fileset_entries'
    )
    def test_run_given_entry_group_id_and_entry_id_should_enrich_a_single_entry(
        self, get_manually_created_fileset_entries, get_entry, parse_gcs_file_patterns,
        create_filtered_data_for_single_bucket, create_filtered_data_for_multiple_buckets,
        create_stats_from_dataframe, create_tag_from_stats):
        get_entry.return_value = self.__make_fake_fileset_entry()

        parse_gcs_file_patterns.return_value = [{'bucket_name': 'my_bucket', 'file_regex': '.*'}]

        dataframe = pd.DataFrame()
        filtered_buckets_stats = {}
        create_filtered_data_for_single_bucket.return_value = (dataframe, filtered_buckets_stats)

        stats = {}
        create_stats_from_dataframe.return_value = stats

        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.run('entry_group_id', 'entry_id')

        get_manually_created_fileset_entries.assert_not_called()
        get_entry.assert_called_once()
        parse_gcs_file_patterns.assert_called_once()
        create_filtered_data_for_single_bucket.assert_called_once()
        create_filtered_data_for_multiple_buckets.assert_not_called()
        create_stats_from_dataframe.assert_called_once()
        create_tag_from_stats.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.create_tag_from_stats')
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_stats_summarizer.GCStorageStatsSummarizer.create_stats_from_dataframe'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_multiple_buckets'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_single_bucket'
    )
    @patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.parse_gcs_file_patterns')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_entry')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_manually_created_fileset_entries'
    )
    def test_run_given_entry_group_id_and_entry_id_and_multiple_gcs_patterns_should_enrich_a_single_entry(
        self, get_manually_created_fileset_entries, get_entry, parse_gcs_file_patterns,
        create_filtered_data_for_single_bucket, create_filtered_data_for_multiple_buckets,
        create_stats_from_dataframe, create_tag_from_stats):
        entry = self.__make_fake_fileset_entry()

        entry.gcs_fileset_spec.file_patterns.append('gs://my_bucket/*csv')

        get_entry.return_value = entry

        parse_gcs_file_patterns.return_value = [{
            'bucket_name': 'my_bucket',
            'file_regex': '.*'
        }, {
            'bucket_name': 'my_bucket',
            'file_regex': '.*csv'
        }]

        dataframe = pd.DataFrame()
        filtered_buckets_stats = {}
        create_filtered_data_for_single_bucket.return_value = (dataframe, filtered_buckets_stats)

        stats = {}
        create_stats_from_dataframe.return_value = stats

        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.run('entry_group_id', 'entry_id')

        get_manually_created_fileset_entries.assert_not_called()
        get_entry.assert_called_once()
        parse_gcs_file_patterns.assert_called_once()
        self.assertEqual(2, create_filtered_data_for_single_bucket.call_count)
        create_filtered_data_for_multiple_buckets.assert_not_called()
        create_stats_from_dataframe.assert_called_once()
        create_tag_from_stats.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.create_tag_from_stats')
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_stats_summarizer.GCStorageStatsSummarizer.create_stats_from_dataframe'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_multiple_buckets'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_single_bucket'
    )
    @patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.parse_gcs_file_patterns')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_entry')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_manually_created_fileset_entries'
    )
    def test_run_given_bucket_with_wildcard_should_call_retrieve_multiple_buckets(
        self, get_manually_created_fileset_entries, get_entry, parse_gcs_file_patterns,
        create_filtered_data_for_single_bucket, create_filtered_data_for_multiple_buckets,
        create_stats_from_dataframe, create_tag_from_stats):
        get_entry.return_value = self.__make_fake_fileset_entry()

        parse_gcs_file_patterns.return_value = [{'bucket_name': 'my_bucket*', 'file_regex': '.*'}]

        dataframe = pd.DataFrame()
        filtered_buckets_stats = {}
        create_filtered_data_for_multiple_buckets.return_value = (dataframe,
                                                                  filtered_buckets_stats)

        stats = {}
        create_stats_from_dataframe.return_value = stats

        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.run('entry_group_id', 'entry_id')

        get_manually_created_fileset_entries.assert_not_called()
        get_entry.assert_called_once()
        parse_gcs_file_patterns.assert_called_once()
        create_filtered_data_for_single_bucket.assert_not_called()
        create_filtered_data_for_multiple_buckets.assert_called_once()
        create_stats_from_dataframe.assert_called_once()
        create_tag_from_stats.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.create_tag_from_stats')
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_stats_summarizer.GCStorageStatsSummarizer.create_stats_from_dataframe'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_multiple_buckets'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_single_bucket'
    )
    @patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.parse_gcs_file_patterns')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_entry')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_manually_created_fileset_entries'
    )
    def test_run_given_bucket_with_wildcard_and_multiple_gcs_patterns_should_call_retrieve_multiple_buckets(
        self, get_manually_created_fileset_entries, get_entry, parse_gcs_file_patterns,
        create_filtered_data_for_single_bucket, create_filtered_data_for_multiple_buckets,
        create_stats_from_dataframe, create_tag_from_stats):
        entry = self.__make_fake_fileset_entry()

        entry.gcs_fileset_spec.file_patterns.append('gs://my_bucket*/*csv')

        get_entry.return_value = entry

        parse_gcs_file_patterns.return_value = [{
            'bucket_name': 'my_bucket*',
            'file_regex': '.*'
        }, {
            'bucket_name': 'my_bucket*',
            'file_regex': '.*csv'
        }]

        dataframe = pd.DataFrame()
        filtered_buckets_stats = {}
        create_filtered_data_for_multiple_buckets.return_value = (dataframe,
                                                                  filtered_buckets_stats)

        stats = {}
        create_stats_from_dataframe.return_value = stats

        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.run('entry_group_id', 'entry_id')

        get_manually_created_fileset_entries.assert_not_called()
        get_entry.assert_called_once()
        parse_gcs_file_patterns.assert_called_once()
        create_filtered_data_for_single_bucket.assert_not_called()
        self.assertEqual(2, create_filtered_data_for_multiple_buckets.call_count)
        create_stats_from_dataframe.assert_called_once()
        create_tag_from_stats.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.create_tag_from_stats')
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_stats_summarizer.GCStorageStatsSummarizer.create_stats_from_dataframe'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_multiple_buckets'
    )
    @patch(
        'datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.create_filtered_data_for_single_bucket'
    )
    @patch('datacatalog_fileset_enricher.gcs_storage_filter.StorageFilter.parse_gcs_file_patterns')
    @patch('datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_entry')
    @patch(
        'datacatalog_fileset_enricher.datacatalog_helper.DataCatalogHelper.get_manually_created_fileset_entries'
    )
    def test_run_given_no_entry_group_id_and_entry_id_should_enrich_multiple_entries(
        self, get_manually_created_fileset_entries, get_entry, parse_gcs_file_patterns,
        create_filtered_data_for_single_bucket, create_filtered_data_for_multiple_buckets,
        create_stats_from_dataframe, create_tag_from_stats):
        get_manually_created_fileset_entries.return_value = [('uscentral-1', 'entry_group_id',
                                                              'entry_id')]

        get_entry.return_value = self.__make_fake_fileset_entry()

        parse_gcs_file_patterns.return_value = [{'bucket_name': 'my_bucket*', 'file_regex': '.*'}]

        dataframe = pd.DataFrame()
        filtered_buckets_stats = {}
        create_filtered_data_for_multiple_buckets.return_value = (dataframe,
                                                                  filtered_buckets_stats)

        stats = {}
        create_stats_from_dataframe.return_value = stats

        datacatalog_fileset_enricher = DatacatalogFilesetEnricher('test_project')
        datacatalog_fileset_enricher.run()

        get_manually_created_fileset_entries.assert_called_once()
        get_entry.assert_called_once()
        parse_gcs_file_patterns.assert_called_once()
        create_filtered_data_for_single_bucket.assert_not_called()
        create_filtered_data_for_multiple_buckets.assert_called_once()
        create_stats_from_dataframe.assert_called_once()
        create_tag_from_stats.assert_called_once()

    @classmethod
    def __make_fake_fileset_entry(cls):
        entry = datacatalog_v1.types.Entry()
        entry.display_name = 'My Fileset'
        entry.description = 'This fileset consists of ....'
        entry.gcs_fileset_spec.file_patterns.append('gs://my_bucket/*')
        entry.type = datacatalog_v1.enums.EntryType.FILESET

        return entry
