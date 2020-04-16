import logging
import re

import pandas as pd

from .gcs_storage_client_helper import StorageClientHelper


class StorageFilter:
    __FILE_PATTERN_REGEX = r'^gs:[\/][\/]([a-zA-Z-_\d*]+)[\/](.*)$'

    def __init__(self, project_id):
        self.__storage_helper = StorageClientHelper(project_id)
        self.__project_id = project_id

    def create_filtered_data_for_multiple_buckets(self,
                                                  bucket_pattern,
                                                  file_regex,
                                                  bucket_prefix=None):
        logging.info('===> Get all Buckets from Cloud Storage...')
        buckets = self.__storage_helper.list_buckets(bucket_prefix)
        logging.info('==== DONE ==================================================')
        logging.info('')

        dataframe = None
        filtered_buckets_stats = []
        filtered_buckets = self.filter_buckets_for_bucket_pattern(buckets, bucket_pattern)
        for bucket in filtered_buckets:
            bucket_name = bucket.name
            logging.info(f'[BUCKET: {bucket_name}')
            logging.info('Get Files information from Cloud Storage...')
            blobs = self.filter_blobs_from_bucket(bucket, file_regex)
            filtered_buckets_stats.append({'bucket_name': bucket_name, 'files': len(blobs)})
            if len(blobs) > 0:
                aux_dataframe = self.create_dataframe_from_blobs(blobs)
                if dataframe is not None:
                    dataframe = dataframe.append(aux_dataframe)
                else:
                    dataframe = aux_dataframe

        return dataframe, filtered_buckets_stats

    def create_filtered_data_for_single_bucket(self, bucket_name, file_regex):
        logging.info(f'===> Get the Bucket: {bucket_name} from Cloud Storage...')
        bucket = self.__storage_helper.get_bucket(bucket_name)

        logging.info('==== DONE ==================================================')
        logging.info('')
        filtered_buckets_stats = []

        if bucket:
            logging.info('Get Files information from Cloud Storage...')
            blobs = self.filter_blobs_from_bucket(bucket, file_regex)
            filtered_buckets_stats.append({'bucket_name': bucket_name, 'files': len(blobs)})
            return self.create_dataframe_from_blobs(blobs), filtered_buckets_stats
        else:
            filtered_buckets_stats.append({
                'bucket_name': bucket_name,
                'files': 0,
                'bucket_not_found': True
            })
            return None, filtered_buckets_stats

    def filter_blobs_from_bucket(self, bucket, file_regex):
        filtered_blobs = []
        blobs = self.__storage_helper.list_blobs(bucket)
        for blob in blobs:
            file_name = blob.name
            re_match = re.match(f'^{file_regex}$', file_name)
            if re_match:
                filtered_blobs.append(blob)

        if len(filtered_blobs) == 0:
            logging.warning(f'Zero files found for bucket: {bucket},'
                            f' with file_pattern: {file_regex}')

        return filtered_blobs

    @classmethod
    def create_dataframe_from_blobs(cls, blobs):
        dataframe = pd.DataFrame(
            [[blob.name, blob.public_url, blob.size, blob.time_created, blob.updated]
             for blob in blobs],
            columns=['name', 'public_url', 'size', 'time_created', 'time_updated'])
        return dataframe

    @classmethod
    def filter_buckets_for_bucket_pattern(cls, buckets, bucket_pattern):
        filtered_buckets = []
        for bucket in buckets:
            bucket_name = bucket.name
            re_match = re.match(f'^{bucket_pattern}$', bucket_name)
            if re_match:
                filtered_buckets.append(bucket)
        return filtered_buckets

    @classmethod
    def convert_str_to_usable_regex(cls, plain_str):
        return plain_str.replace('*', '.*')

    @classmethod
    def parse_gcs_file_patterns(cls, gcs_file_patterns):
        parsed_gcs_patterns = []
        for gcs_file_pattern in gcs_file_patterns:
            re_match = re.match(cls.__FILE_PATTERN_REGEX, gcs_file_pattern)
            if re_match:
                bucket_name, gcs_file_pattern = re_match.groups()
                parsed_gcs_patterns.append({
                    'bucket_name':
                    cls.convert_str_to_usable_regex(bucket_name),
                    'file_regex':
                    cls.convert_str_to_usable_regex(gcs_file_pattern)
                })
        return parsed_gcs_patterns
