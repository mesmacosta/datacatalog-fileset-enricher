import logging
import re

import pandas as pd

from .datacatalog_helper import DataCatalogHelper
from .gcs_storage_client_helper import StorageClientHelper

"""
 The Fileset Enhancer relies on the file_pattern created on the Entry.

 Those are examples of valid file_patterns:

  `gs://bucket_name/*`: matches all files in `bucket_name`
  `gs://bucket_name/file*`: matches files prefixed by `file` in
                              `bucket_name`
  `gs://bucket_name/a/*/b`: matches all files in `bucket_name` that match
                              `a/*/b` pattern, such as `a/c/b`, `a/d/b`
  `gs://another_bucket/a.txt`: matches `gs://another_bucket/a.txt`
"""


class DatacatalogFilesetEnricher:
    __FILE_PATTERN_REGEX = r'^gs:[\/][\/]([a-zA-Z-_\d*]+)[\/](.*)$'

    def __init__(self, project_id):
        self.__storage_helper = StorageClientHelper(project_id)
        self.__dacatalog_helper = DataCatalogHelper(project_id)
        self.__project_id = project_id

    def clean_up_all(self):
        logging.info(f'===> Clean up started')

        self.__dacatalog_helper.delete_tag_template()
        logging.info(f'Template and Tags deleted...')

        self.__dacatalog_helper.delete_entries_and_entry_groups()
        logging.info('==== DONE ==================================================')

    def clean_up_fileset_template_and_tags(self):
        logging.info(f'===> Clean up started')

        self.__dacatalog_helper.delete_tag_template()
        logging.info(f'Template and Tags deleted...')

    def run(self, entry_group_id=None, entry_id=None):
        # If the entry_group_id and entry_id are provided we enrich just this entry,
        # otherwise we retrieve the Fileset Entries using search
        if entry_group_id and entry_id:
            self.enrich_datacatalog_fileset_entry(entry_group_id, entry_id)
        else:
            logging.info(f'===> Retrieving manually created Fileset Entries'
                         f' project: {self.__project_id}')
            logging.info('')
            entries = self.__dacatalog_helper.get_manually_created_fileset_entries()

            logging.info(f'{len(entries)} Entries will be processed...')
            logging.info('')

            for entry_group_id, entry_id in entries:
                self.enrich_datacatalog_fileset_entry(entry_group_id, entry_id)

    def enrich_datacatalog_fileset_entry(self, entry_group_id, entry_id):
        logging.info('')
        logging.info(f'[ENTRY_GROUP: {entry_group_id}]')
        logging.info(f'[ENTRY: {entry_id}]')
        logging.info(f'===> Enrich Fileset Entry metadata with tags')
        logging.info('')
        logging.info('===> Get Entry from DataCatalog...')
        entry = self.__dacatalog_helper.get_entry(entry_group_id, entry_id)
        file_pattern = entry.gcs_fileset_spec.file_patterns[0]

        logging.info('==== DONE ==================================================')
        logging.info('')

        # Split the file pattern into bucket_name and file_regex.
        parsed_gcs_pattern = DatacatalogFilesetEnricher.parse_gcs_file_pattern(file_pattern)

        bucket_name = parsed_gcs_pattern['bucket_name']

        bucket = None
        dataframe = None
        # If we have a wildcard on the bucket_name, we have to retrieve all buckets from the project
        if '*' in bucket_name:
            pass
        else:
            logging.info('===> Get the Bucket from DataCatalog...')
            bucket = self.__storage_helper.get_bucket(bucket_name)

            logging.info('==== DONE ==================================================')
            logging.info('')

            if bucket:
                logging.info('Get Files information from Cloud Storage...')
                blobs = self.filter_blobs_from_bucket(bucket, parsed_gcs_pattern["file_regex"])
                dataframe = self.create_dataframe_from_blobs(blobs)

        logging.info('===> Generate Fileset statistics...')
        stats = self.create_stats_from_dataframe(dataframe, file_pattern)

        # ADD info about not existing buckets, to show users they used an invalid bucket name
        if not bucket:
            stats['bucket_not_found'] = True

        logging.info('==== DONE ==================================================')
        logging.info('')

        logging.info('===> Create Tag on DataCatalog...')
        self.__dacatalog_helper.create_tag_from_stats(entry, stats)
        logging.info('==== DONE ==================================================')
        logging.info('')

    def filter_blobs_from_bucket(self, bucket, file_regex):
        filtered_blobs = []
        blobs = self.__storage_helper.list_blobs(bucket)
        for blob in blobs:
            file_name = blob.name
            re_match = re.match(f'^{file_regex}$', file_name)
            if re_match:
                filtered_blobs.append(blob)
        return filtered_blobs

    @classmethod
    def create_stats_from_dataframe(cls, dataframe, prefix):
        if dataframe is not None:
            size = dataframe['size']
            time_created = dataframe['time_created']
            time_updated = dataframe['time_updated']
            stats = {
                'count': len(dataframe),
                'min_size': size.min(),
                'max_size': size.max(),
                'avg_size': size.mean(),
                'min_created': time_created.min(),
                'max_created': time_created.max(),
                'min_updated': time_updated.min(),
                'max_updated': time_updated.max(),
                'created_files_by_day': cls.get_daily_stats(time_created, 'time_created'),
                'updated_files_by_day': cls.get_daily_stats(time_updated, 'time_updated'),
                'prefix': prefix
            }
        else:
            stats = {
                'count': 0,
                'prefix': prefix
            }

        return stats

    @classmethod
    def create_dataframe_from_blobs(cls, blobs):
        dataframe = pd.DataFrame([[blob.name, blob.public_url, blob.size, blob.time_created,
                                   blob.updated] for blob in blobs],
                                 columns=['name', 'public_url',
                                          'size', 'time_created', 'time_updated'])
        return dataframe

    @classmethod
    def convert_file_pattern_to_regex(cls, file_pattern):
        return file_pattern.replace('*', '.*')

    @classmethod
    def get_daily_stats(cls, series, timestamp_column):
        time_created_same_day = series.apply(lambda timestamp: timestamp._date_repr).to_frame()
        value = ''
        for day, count in time_created_same_day[timestamp_column].value_counts().iteritems():
            value += f'{day} [count: {count}], '
        return value[:-2]

    @classmethod
    def parse_gcs_file_pattern(cls, gcs_file_pattern):
        re_match = re.match(cls.__FILE_PATTERN_REGEX, gcs_file_pattern)
        if re_match:
            bucket_name, gcs_file_pattern = re_match.groups()
            return {'bucket_name': bucket_name,
                    'file_regex': cls.convert_file_pattern_to_regex(gcs_file_pattern)}
