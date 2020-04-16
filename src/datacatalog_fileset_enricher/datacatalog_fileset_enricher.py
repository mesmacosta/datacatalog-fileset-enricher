import logging

import pandas as pd

from google.api_core.exceptions import AlreadyExists

from .datacatalog_helper import DataCatalogHelper
from .gcs_storage_filter import StorageFilter
from .gcs_storage_stats_summarizer import GCStorageStatsSummarizer
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
    # Default location.
    __LOCATION = 'us-central1'
    __FILE_PATTERN_REGEX = r'^gs:[\/][\/]([a-zA-Z-_\d*]+)[\/](.*)$'

    def __init__(self, project_id):
        self.__storage_filter = StorageFilter(project_id)
        self.__dacatalog_helper = DataCatalogHelper(project_id)
        self.__project_id = project_id

    def create_template(self, location):
        logging.info(f'===> Create Template started')

        tag_template_name = self.__dacatalog_helper.get_tag_template_name(location=location)

        try:
            self.__dacatalog_helper.create_fileset_enricher_tag_template(tag_template_name)
        except AlreadyExists:
            logging.warning(f'Template {tag_template_name} already exists')

        logging.info('==== DONE ==================================================')

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

    def run(self,
            entry_group_id=None,
            entry_id=None,
            tag_fields=None,
            bucket_prefix=None,
            tag_template_name=None):
        # If the entry_group_id and entry_id are provided we enrich just this entry,
        # otherwise we retrieve the Fileset Entries using search
        if entry_group_id and entry_id:
            self.enrich_datacatalog_fileset_entry(self.__LOCATION, entry_group_id, entry_id,
                                                  tag_fields, bucket_prefix, tag_template_name)
        else:
            logging.info(f'===> Retrieving manually created Fileset Entries'
                         f' project: {self.__project_id}')
            logging.info('')
            entries = self.__dacatalog_helper.get_manually_created_fileset_entries()

            logging.info(f'{len(entries)} Entries will be processed...')
            logging.info('')

            for location, entry_group_id, entry_id in entries:
                self.enrich_datacatalog_fileset_entry(location, entry_group_id, entry_id,
                                                      tag_fields, bucket_prefix, tag_template_name)

    def enrich_datacatalog_fileset_entry(self,
                                         location,
                                         entry_group_id,
                                         entry_id,
                                         tag_fields=None,
                                         bucket_prefix=None,
                                         tag_template_name=None):
        logging.info('')
        logging.info(f'[LOCATION: {location}]')
        logging.info(f'[ENTRY_GROUP: {entry_group_id}]')
        logging.info(f'[ENTRY: {entry_id}]')
        logging.info(f'===> Enrich Fileset Entry metadata with tags')
        logging.info('')
        logging.info('===> Get Entry from DataCatalog...')
        entry = self.__dacatalog_helper.get_entry(location, entry_group_id, entry_id)
        file_patterns = list(entry.gcs_fileset_spec.file_patterns)

        logging.info('==== DONE ==================================================')
        logging.info('')

        # Split the file pattern into bucket_name and file_regex.
        parsed_gcs_patterns = self.__storage_filter.parse_gcs_file_patterns(file_patterns)

        execution_time = pd.Timestamp.utcnow()
        dataframe, filtered_buckets_stats = self.__create_dataframe_for_parsed_gcs_patterns(
            parsed_gcs_patterns, bucket_prefix)

        logging.info('===> Generate Fileset statistics...')
        stats = GCStorageStatsSummarizer.create_stats_from_dataframe(dataframe, file_patterns,
                                                                     filtered_buckets_stats,
                                                                     execution_time, bucket_prefix)

        logging.info('==== DONE ==================================================')
        logging.info('')

        logging.info('===> Create Tags on DataCatalog from Fileset statistics...')
        self.__dacatalog_helper.create_tag_from_stats(entry, stats, tag_fields, tag_template_name)
        logging.info('==== DONE ==================================================')
        logging.info('')

    def __create_dataframe_for_parsed_gcs_patterns(self, parsed_gcs_patterns, bucket_prefix):
        dataframe = None
        filtered_buckets_stats = []
        for parsed_gcs_pattern in parsed_gcs_patterns:

            bucket_name = parsed_gcs_pattern['bucket_name']

            # If we have a wildcard on the bucket_name,
            # we have to retrieve all buckets from the project
            if '*' in bucket_name:
                aux_dataframe, inner_filtered_buckets_stats = self.__storage_filter. \
                    create_filtered_data_for_multiple_buckets(bucket_name, parsed_gcs_pattern[
                        "file_regex"], bucket_prefix)
                if dataframe is not None:
                    dataframe = dataframe.append(aux_dataframe)
                else:
                    dataframe = aux_dataframe
                # We are dealing with a list of buckets so we extend it
                filtered_buckets_stats.extend(inner_filtered_buckets_stats)

            else:
                aux_dataframe, inner_filtered_buckets_stats = self.__storage_filter. \
                    create_filtered_data_for_single_bucket(bucket_name,
                                                           parsed_gcs_pattern["file_regex"])
                if dataframe is not None:
                    dataframe = dataframe.append(aux_dataframe)
                else:
                    dataframe = aux_dataframe
                # We are dealing with a list of buckets so we extend it
                filtered_buckets_stats.extend(inner_filtered_buckets_stats)

        return dataframe, filtered_buckets_stats
