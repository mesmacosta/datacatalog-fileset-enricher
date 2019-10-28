import logging

from google.cloud import storage
from google.api_core import exceptions


class StorageClientHelper:

    def __init__(self, project_id):
        self.__storage_cloud_client = storage.Client(project=project_id)

    def get_bucket(self, name):
        try:
            return self.__storage_cloud_client.get_bucket(name)
        except (exceptions.Forbidden, exceptions.NotFound):
            logging.info(f'Bucket: {name} does not exist')
            return None

    def list_buckets(self, prefix=None):
        results_iterator = self.__storage_cloud_client.list_buckets(prefix=prefix)

        results = []
        for page in results_iterator.pages:
            results.extend(page)

        return results

    def list_blobs(self, bucket, prefix=None):
        results_iterator = self.__storage_cloud_client.list_blobs(bucket, prefix=prefix)

        results = []
        for page in results_iterator.pages:
            results.extend(page)

        return results
