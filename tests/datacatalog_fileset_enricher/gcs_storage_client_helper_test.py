from unittest import TestCase
from unittest.mock import patch

from google.api_core import exceptions

from datacatalog_fileset_enricher.gcs_storage_client_helper import StorageClientHelper


@patch('google.cloud.storage.Client.__init__', lambda self, **kargs: None)
class StorageClientHelperTestCase(TestCase):

    @patch('google.cloud.storage.Client.get_bucket')
    def test_get_bucket_should_return_bucket(self, get_bucket):

        storage_client = StorageClientHelper('test_project')
        bucket = storage_client.get_bucket('my_bucket')
        self.assertIsNotNone(bucket)
        get_bucket.assert_called_once()

    @patch('google.cloud.storage.Client.get_bucket')
    def test_get_bucket_on_exception_should_not_leak_error(self, get_bucket):

        get_bucket.side_effect = exceptions.NotFound('error on retrieving bucket')

        storage_client = StorageClientHelper('test_project')
        bucket = storage_client.get_bucket('my_bucket')
        self.assertIsNone(bucket)
        get_bucket.assert_called_once()

    @patch('google.cloud.storage.Client.list_buckets')
    def test_list_buckets_should_return_buckets(self, list_buckets):

        results_iterator = MockedObject()
        results_iterator.pages = [{}]

        list_buckets.return_value = results_iterator

        storage_client = StorageClientHelper('test_project')
        buckets = storage_client.list_buckets()
        self.assertIsNotNone(buckets)
        list_buckets.assert_called_once()

    @patch('google.cloud.storage.Client.list_blobs')
    def test_list_blobs_should_return_blobs(self, list_blobs):

        results_iterator = MockedObject()
        results_iterator.pages = [{}]

        list_blobs.return_value = results_iterator

        storage_client = StorageClientHelper('test_project')
        buckets = storage_client.list_blobs('my_bucket')
        self.assertIsNotNone(buckets)
        list_blobs.assert_called_once()


class MockedObject(object):

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]