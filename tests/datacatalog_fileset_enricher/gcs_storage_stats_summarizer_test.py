import pandas as pd
from unittest import TestCase

from datacatalog_fileset_enricher.gcs_storage_stats_summarizer import GCStorageStatsSummarizer


class GCStorageStatsSummarizerTestCase(TestCase):

    def test_create_stats_from_dataframe_with_no_dataframe_should_summarize_the_bucket_stats(self):
        dataframe = None
        filtered_buckets_stats = [{'bucket_name': 'my_bucket', 'files': 100}]
        execution_time = pd.Timestamp.utcnow()
        bucket_prefix = None

        stats = GCStorageStatsSummarizer.create_stats_from_dataframe(dataframe,
                                                                     ['gs://my_bucket/*'],
                                                                     filtered_buckets_stats,
                                                                     execution_time, bucket_prefix)
        self.assertEqual(0, stats['count'])
        self.assertEqual('gs://my_bucket/*', stats['prefix'])
        self.assertEqual('my_bucket [count: 100]', stats['files_by_bucket'])
        self.assertEqual(1, stats['buckets_found'])
        self.assertEqual(execution_time, stats['execution_time'])
        self.assertEqual(None, stats['bucket_prefix'])

    def test_create_stats_from_dataframe_with_no_dataframe_and_no_bucket_stats_should_summarize_the_bucket_stats(  # noqa: E501
        self):
        dataframe = None
        filtered_buckets_stats = []
        execution_time = pd.Timestamp.utcnow()
        bucket_prefix = None

        stats = GCStorageStatsSummarizer.create_stats_from_dataframe(dataframe,
                                                                     ['gs://my_bucket/*'],
                                                                     filtered_buckets_stats,
                                                                     execution_time, bucket_prefix)
        self.assertEqual(0, stats['count'])
        self.assertEqual('gs://my_bucket/*', stats['prefix'])
        self.assertEqual('bucket_not_found', stats['files_by_bucket'])
        self.assertEqual(0, stats['buckets_found'])
        self.assertEqual(execution_time, stats['execution_time'])
        self.assertEqual(None, stats['bucket_prefix'])

    def test_create_stats_from_dataframe_with_no_dataframe_with_bucket_prefix_should_summarize_the_bucket_stats(  # noqa: E501
        self):
        dataframe = None
        filtered_buckets_stats = [{'bucket_name': 'my_bucket', 'files': 100}]
        execution_time = pd.Timestamp.utcnow()
        bucket_prefix = 'my_b'

        stats = GCStorageStatsSummarizer.create_stats_from_dataframe(dataframe,
                                                                     ['gs://my_bucket/*'],
                                                                     filtered_buckets_stats,
                                                                     execution_time, bucket_prefix)
        self.assertEqual(0, stats['count'])
        self.assertEqual('gs://my_bucket/*', stats['prefix'])
        self.assertEqual('my_bucket [count: 100]', stats['files_by_bucket'])
        self.assertEqual(1, stats['buckets_found'])
        self.assertEqual(execution_time, stats['execution_time'])
        self.assertEqual(bucket_prefix, stats['bucket_prefix'])

    def test_create_stats_from_dataframe_with_dataframe_should_summarize_the_bucket_stats(self):
        filtered_buckets_stats = [{'bucket_name': 'my_bucket', 'files': 100}]
        execution_time = pd.Timestamp.utcnow()
        bucket_prefix = None

        blob = MockedObject()
        blob.name = 'my_file'
        blob.public_url = 'https://my_file'
        blob.size = 100000
        blob.time_created = execution_time
        blob.updated = execution_time

        blob_2 = MockedObject()
        blob_2.name = 'my_file_2.csv'
        blob_2.public_url = 'https://my_file_2'
        blob_2.size = 50000
        blob_2.time_created = execution_time
        blob_2.updated = execution_time

        blobs = [blob, blob_2]

        dataframe = pd.DataFrame(
            [[blob.name, blob.public_url, blob.size, blob.time_created, blob.updated]
             for blob in blobs],
            columns=['name', 'public_url', 'size', 'time_created', 'time_updated'])

        stats = GCStorageStatsSummarizer.create_stats_from_dataframe(dataframe,
                                                                     ['gs://my_bucket/*'],
                                                                     filtered_buckets_stats,
                                                                     execution_time, bucket_prefix)
        self.assertEqual(2, stats['count'])
        self.assertEqual('gs://my_bucket/*', stats['prefix'])
        self.assertEqual('my_bucket [count: 100]', stats['files_by_bucket'])
        self.assertEqual(1, stats['buckets_found'])
        self.assertEqual(execution_time, stats['execution_time'])
        self.assertEqual(0.05, stats['min_size'])
        self.assertEqual(0.1, stats['max_size'])
        self.assertEqual(0.07, stats['avg_size'])
        self.assertEqual(0.15, stats['total_size'])
        self.assertEqual(execution_time, stats['min_created'])
        self.assertEqual(execution_time, stats['max_created'])
        self.assertEqual(execution_time, stats['min_updated'])
        self.assertEqual(execution_time, stats['max_updated'])
        self.assertIsNotNone(stats['created_files_by_day'])
        self.assertIsNotNone(stats['updated_files_by_day'])
        self.assertIn('csv [count: 1]', stats['files_by_type'])
        self.assertIn('unknown_file_type [count: 1]', stats['files_by_type'])
        self.assertEqual(None, stats['bucket_prefix'])


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
