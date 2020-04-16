import pandas as pd

from unittest import TestCase
from unittest.mock import patch

from datacatalog_fileset_enricher.gcs_storage_filter import StorageFilter


@patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.__init__',
       lambda self, *args: None)
class StorageFilterTestCase(TestCase):

    @patch(
        'datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_buckets')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_blobs')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.get_bucket')
    def test_create_filtered_data_for_multiple_buckets_with_a_matching_bucket_should_create_filtered_data(  # noqa: E501
        self, get_bucket, list_blobs, list_buckets):
        execution_time = pd.Timestamp.utcnow()

        bucket = MockedObject()
        bucket.name = 'my_bucket'

        bucket_2 = MockedObject()
        bucket_2.name = 'my_bucket_2'

        bucket_3 = MockedObject()
        bucket_3.name = 'invalid_my_bucket_3'

        list_buckets.return_value = [bucket, bucket_2, bucket_3]

        blob = MockedObject()
        blob.name = 'my_file'
        blob.public_url = 'https://my_file'
        blob.size = 100000
        blob.time_created = execution_time
        blob.updated = execution_time

        blob_2 = MockedObject()
        blob_2.name = 'my_file_2'
        blob_2.public_url = 'https://my_file_2'
        blob_2.size = 50000
        blob_2.time_created = execution_time
        blob_2.updated = execution_time

        blobs = [blob, blob_2]

        list_blobs.return_value = blobs

        storage_filter = StorageFilter('test_project')
        dataframe, filtered_buckets_stats = storage_filter.\
            create_filtered_data_for_multiple_buckets(
                'my_bucket.*', '.*')

        self.assertEqual(4, len(dataframe))

        bucket_stats = filtered_buckets_stats[0]
        self.assertEqual('my_bucket', bucket_stats['bucket_name'])
        self.assertEqual(2, bucket_stats['files'])
        self.assertEqual(None, bucket_stats.get('bucket_not_found'))

        get_bucket.assert_not_called()
        self.assertEqual(2, list_blobs.call_count)
        list_buckets.assert_called_once()

    @patch(
        'datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_buckets')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_blobs')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.get_bucket')
    def test_create_filtered_data_for_single_bucket_with_a_existent_bucket_should_create_filtered_data(  # noqa: E501
        self, get_bucket, list_blobs, list_buckets):

        execution_time = pd.Timestamp.utcnow()

        blob = MockedObject()
        blob.name = 'my_file'
        blob.public_url = 'https://my_file'
        blob.size = 100000
        blob.time_created = execution_time
        blob.updated = execution_time

        blob_2 = MockedObject()
        blob_2.name = 'my_file_2'
        blob_2.public_url = 'https://my_file_2'
        blob_2.size = 50000
        blob_2.time_created = execution_time
        blob_2.updated = execution_time

        blobs = [blob, blob_2]

        list_blobs.return_value = blobs

        storage_filter = StorageFilter('test_project')
        dataframe, filtered_buckets_stats = storage_filter.create_filtered_data_for_single_bucket(
            'my_bucket', '.*')

        self.assertEqual(2, len(dataframe))

        first_row = dataframe.loc[0]

        self.assertEqual(first_row['name'], blob.name)
        self.assertEqual(first_row['public_url'], blob.public_url)
        self.assertEqual(first_row['time_created'], blob.time_created)
        self.assertEqual(first_row['time_updated'], blob.updated)

        second_row = dataframe.loc[1]

        self.assertEqual(second_row['name'], blob_2.name)
        self.assertEqual(second_row['public_url'], blob_2.public_url)
        self.assertEqual(second_row['time_created'], blob_2.time_created)
        self.assertEqual(second_row['time_updated'], blob_2.updated)

        bucket_stats = filtered_buckets_stats[0]
        self.assertEqual('my_bucket', bucket_stats['bucket_name'])
        self.assertEqual(2, bucket_stats['files'])
        self.assertEqual(None, bucket_stats.get('bucket_not_found'))

        get_bucket.assert_called_once()
        list_blobs.assert_called_once()
        list_buckets.assert_not_called()

    @patch(
        'datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_buckets')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.list_blobs')
    @patch('datacatalog_fileset_enricher.gcs_storage_client_helper.StorageClientHelper.get_bucket')
    def test_create_filtered_data_for_single_bucket_with_nonexistent_bucket_should_create_filtered_data(  # noqa: E501
        self, get_bucket, list_blobs, list_buckets):

        get_bucket.return_value = None

        storage_filter = StorageFilter('test_project')
        dataframe, filtered_buckets_stats = storage_filter.create_filtered_data_for_single_bucket(
            'my_bucket', '.*')

        self.assertEqual(None, dataframe)

        bucket_stats = filtered_buckets_stats[0]
        self.assertEqual('my_bucket', bucket_stats['bucket_name'])
        self.assertEqual(0, bucket_stats['files'])
        self.assertEqual(True, bucket_stats['bucket_not_found'])
        get_bucket.assert_called_once()
        list_blobs.assert_not_called()
        list_buckets.assert_not_called()

    def test_parse_gcs_file_pattern_should_split_bucket_name_and_file_pattern(self):
        storage_filter = StorageFilter('test_project')
        parsed_gcs_file_pattern = storage_filter.parse_gcs_file_patterns(['gs://my_bucket*/*'])[0]
        self.assertEqual('my_bucket.*', parsed_gcs_file_pattern['bucket_name'])
        self.assertEqual('.*', parsed_gcs_file_pattern['file_regex'])


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
