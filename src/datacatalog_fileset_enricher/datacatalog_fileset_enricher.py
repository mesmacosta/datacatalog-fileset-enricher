import logging

from .datacatalog_helper import DataCatalogHelper
from .gcs_storage_filter import GCStorageFilter
from .gcs_storage_stats_reducer import GCStorageStatsReducer

"""
 The Fileset Enhancer relies on the file_pattern created on the Entry.

 Those are examples of valid file_patterns:

  `gs://bucket_name/*`: matches all files in `bucket_name`
  `gs://bucket_name/file*`: matches files prefixed by `file` in
                              `bucket_name`
  `gs://bucket_name/a/*/b`: matches all files in `bucket_name` that match
                              `a/*/b` pattern, such as `a/c/b`, `a/d/b`
  `gs://another_bucket/a.txt`: matches `gs://another_bucket/a.txt`
  `gs://*/a.txt`: matches all buckets and all files named a.txt
  `gs://*name/a.txt`: matches all buckets that ends with name and all files named a.txt
"""


class DatacatalogFilesetEnricher:
    __FILE_PATTERN_REGEX = r'^gs:[\/][\/]([a-zA-Z-_\d*]+)[\/](.*)$'

    def __init__(self, project_id):
        self.__storage_filter = GCStorageFilter(project_id)
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
        parsed_gcs_pattern = self.__storage_filter.parse_gcs_file_pattern(file_pattern)

        bucket_name = parsed_gcs_pattern['bucket_name']

        bucket = None
        # If we have a wildcard on the bucket_name, we have to retrieve all buckets from the project
        if '*' in bucket_name:
            dataframe, filtered_buckets_stats = self.__storage_filter. \
                create_filtered_data_for_multiple_buckets(bucket_name, parsed_gcs_pattern[
                "file_regex"])

        else:
            dataframe, filtered_buckets_stats = self.__storage_filter. \
                create_filtered_data_for_single_bucket(bucket_name,
                                                       parsed_gcs_pattern["file_regex"])

        logging.info('===> Generate Fileset statistics...')
        stats = GCStorageStatsReducer.create_stats_from_dataframe(dataframe, file_pattern,
                                                                  filtered_buckets_stats)

        # ADD info about not existing buckets, to show users they used an invalid bucket name
        if not bucket:
            stats['bucket_not_found'] = True

        logging.info('==== DONE ==================================================')
        logging.info('')

        logging.info('===> Create Tag on DataCatalog...')
        self.__dacatalog_helper.create_tag_from_stats(entry, stats)
        logging.info('==== DONE ==================================================')
        logging.info('')
