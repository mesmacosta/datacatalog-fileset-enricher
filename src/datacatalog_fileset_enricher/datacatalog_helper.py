"""Helper to call datacatalog_v1beta1 api methods."""
import logging
import re

from google.api_core.exceptions import PermissionDenied
from google.cloud import datacatalog_v1beta1


class DataCatalogHelper:
    """
    DataCatalogHelper enables calls to datacatalog_v1beta1
    """

    __AVALIABLE_TAG_FIELDS = ['files', 'min_file_size', 'max_file_size', 'avg_file_size',
                              'first_created_date', 'last_created_date', 'last_updated_date',
                              'created_files_by_day', 'updated_files_by_day', 'prefix',
                              'buckets_found', 'files_by_bucket']
    __ENTRY_NAME_PATTERN = r'^projects[\/][a-zA-Z-\d]+[\/]locations[\/][a-zA-Z-\d]+[' \
                           r'\/]entryGroups[\/]([@a-zA-Z-_\d]+)[\/]entries[\/]([a-zA-Z_\d-]+)$'
    __MANUALLY_CREATED_FILESET_ENTRIES_SEARCH_QUERY = \
        'not name:crawler AND projectId=$project_id AND type=fileset'
    __LOCATION = 'us-central1'
    __TAG_TEMPLATE = 'fileset_enricher_tag_template'

    def __init__(self, project_id):
        self.__datacatalog = datacatalog_v1beta1.DataCatalogClient()
        self.__project_id = project_id

    def create_fileset_enricher_tag_template(self):
        tag_template = datacatalog_v1beta1.types.TagTemplate()
        tag_template.display_name = 'Tag Template to enrich the GCS Fileset metadata'
        tag_template.fields['files'].display_name = 'Number of files found'
        tag_template.fields['files'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE.value

        tag_template.fields['min_file_size'].display_name = 'Minimum file size found in bytes'
        tag_template.fields['min_file_size'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE.value

        tag_template.fields['max_file_size'].display_name = 'Maximum file size found in bytes'
        tag_template.fields['max_file_size'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE.value

        tag_template.fields['avg_file_size'].display_name = 'Average file size found in bytes'
        tag_template.fields['avg_file_size'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE.value

        tag_template.fields['first_created_date'].display_name = \
            'First time a file was created in the buckets'
        tag_template.fields['first_created_date'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.TIMESTAMP.value

        tag_template.fields['last_created_date'].display_name = \
            'Last time a file was created in the buckets'
        tag_template.fields['last_created_date'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.TIMESTAMP.value

        tag_template.fields['last_updated_date'].display_name = \
            'Last time a file was updated in the buckets'
        tag_template.fields['last_updated_date'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.TIMESTAMP.value

        tag_template.fields['created_files_by_day'].display_name = \
            'Number of files created on the same date'
        tag_template.fields['created_files_by_day'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['updated_files_by_day'].display_name = \
            'Number of files updated on the same date'
        tag_template.fields['updated_files_by_day'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['prefix'].display_name = \
            'Prefix used to find the files'
        tag_template.fields['prefix'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['buckets_found'].display_name = \
            'Number of buckets that matched the prefix'
        tag_template.fields['buckets_found'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.DOUBLE.value

        tag_template.fields['files_by_bucket'].display_name = \
            'Number of files found on each bucket'
        tag_template.fields['files_by_bucket'].type.primitive_type = \
            datacatalog_v1beta1.enums.FieldType.PrimitiveType.STRING.value

        return self.__datacatalog.create_tag_template(
            parent=datacatalog_v1beta1.DataCatalogClient.location_path(
                self.__project_id, DataCatalogHelper.__LOCATION),
            tag_template_id=DataCatalogHelper.__TAG_TEMPLATE,
            tag_template=tag_template)

    def create_tag_from_stats(self, entry, stats, tag_fields=None):
        # Create tag_template.
        try:
            tag_template = self.get_fileset_enricher_tag_template()
        except PermissionDenied:
            tag_template = self.create_fileset_enricher_tag_template()

        tag = datacatalog_v1beta1.types.Tag()
        tag.template = tag_template.name

        tag.fields['prefix'].string_value = stats['prefix']
        count = stats['count']
        tag.fields['files'].double_value = count

        tag.fields['files_by_bucket'].string_value = stats['files_by_bucket']
        tag.fields['buckets_found'].double_value = stats['buckets_found']

        # If we don't have files, then we don't have stats about the files
        if count > 0:
            tag.fields['min_file_size'].double_value = stats['min_size']
            tag.fields['max_file_size'].double_value = stats['max_size']
            tag.fields['avg_file_size'].double_value = stats['avg_size']
            tag.fields['first_created_date'].timestamp_value.FromJsonString(stats['min_created']
                                                                            .isoformat())
            tag.fields['last_created_date'].timestamp_value.FromJsonString(stats['max_created']
                                                                           .isoformat())
            tag.fields['last_updated_date'].timestamp_value.FromJsonString(stats['max_updated']
                                                                           .isoformat())
            tag.fields['created_files_by_day'].string_value = stats['created_files_by_day']
            tag.fields['updated_files_by_day'].string_value = stats['updated_files_by_day']

        if tag_fields:
            non_used_tag_fields = set(DataCatalogHelper.__AVALIABLE_TAG_FIELDS).\
                difference(set(tag_fields))

            for field in non_used_tag_fields:
                try:
                    del tag.fields[field]
                except KeyError:
                    # In protobufs there's no way to check if a field exists before deleting,
                    # so we capture KeyError errors.
                    pass

        self.synchronize_entry_tags(entry, [tag])

    def delete_entries_and_entry_groups(self):
        scope = datacatalog_v1beta1.types.SearchCatalogRequest.Scope()
        scope.include_project_ids.extend([self.__project_id])

        query = DataCatalogHelper.__MANUALLY_CREATED_FILESET_ENTRIES_SEARCH_QUERY.replace(
            '$project_id',
            self.__project_id)

        search_results = [result for result in
                          self.__datacatalog.search_catalog(scope=scope,
                                                            query=query,
                                                            order_by='relevance',
                                                            page_size=1000)]
        datacatalog_entry_name_pattern = '(?P<entry_group_name>.+?)/entries/(.+?)'

        entry_group_names = []
        for result in search_results:
            try:
                if '@' not in result.relative_resource_name:
                    self.__datacatalog.delete_entry(result.relative_resource_name)
                    logging.info(f'Entry deleted: {result.relative_resource_name}')
                    entry_group_name = re.match(
                        pattern=datacatalog_entry_name_pattern,
                        string=result.relative_resource_name
                    ).group('entry_group_name')
                    entry_group_names.append(entry_group_name)
            except:  # noqa: E722
                logging.exception('Exception deleting entry')

        # Delete any pre-existing Entry Groups.
        for entry_group_name in set(entry_group_names):
            try:
                if '@' not in entry_group_name:
                    self.__datacatalog.delete_entry_group(entry_group_name)
                    logging.info(f'Entry Group deleted: {entry_group_name}')
            except:  # noqa: E722
                logging.exception('Exception deleting entry Group')

    def delete_tag_template(self):
        name = datacatalog_v1beta1.DataCatalogClient.tag_template_path(self.__project_id,
                                                                       DataCatalogHelper.
                                                                       __LOCATION,
                                                                       DataCatalogHelper.
                                                                       __TAG_TEMPLATE)
        try:
            self.__datacatalog.delete_tag_template(name, force=True)
        except:  # noqa: E722
            logging.exception('Exception deleting Tag Template')

    def delete_tag(self, entry_group_id, entry_id):
        name = datacatalog_v1beta1.DataCatalogClient.tag_path(self.__project_id,
                                                              DataCatalogHelper.__LOCATION,
                                                              entry_group_id, entry_id,
                                                              DataCatalogHelper.__TAG_TEMPLATE)
        self.__datacatalog.delete_tag(name)

    def get_entry(self, entry_group_id, entry_id):
        name = datacatalog_v1beta1.DataCatalogClient.entry_path(self.__project_id,
                                                                DataCatalogHelper.__LOCATION,
                                                                entry_group_id, entry_id)
        return self.__datacatalog.get_entry(name)

    def get_fileset_enricher_tag_template(self):
        return self.__datacatalog.get_tag_template(
            datacatalog_v1beta1.DataCatalogClient.tag_template_path(
                self.__project_id, DataCatalogHelper.__LOCATION, DataCatalogHelper.__TAG_TEMPLATE))

    # Currently we don't have a list method, so we are using search which is not exhaustive,
    # and might not return some entries.
    def get_manually_created_fileset_entries(self):
        scope = datacatalog_v1beta1.types.SearchCatalogRequest.Scope()
        scope.include_project_ids.extend([self.__project_id])

        query = DataCatalogHelper.__MANUALLY_CREATED_FILESET_ENTRIES_SEARCH_QUERY.replace(
            '$project_id',
            self.__project_id)

        search_results = self.__datacatalog.search_catalog(scope=scope, query=query,
                                                           order_by='relevance',
                                                           page_size=1000)

        fileset_entries = []
        for result in search_results:
            re_match = re.match(pattern=DataCatalogHelper.__ENTRY_NAME_PATTERN,
                                string=result.relative_resource_name)
            if re_match:
                entry_group_id, entry_id, = re_match.groups()
                fileset_entries.append((entry_group_id, entry_id))

        return fileset_entries

    def synchronize_entry_tags(self, entry, updated_tags):
        if not updated_tags or len(updated_tags) == 0:
            return

        current_tags = self.__datacatalog.list_tags(parent=entry.name)

        for updated_tag in updated_tags:
            tag_to_create = updated_tag
            tag_to_update = None
            for current_tag in current_tags:
                logging.info(f'Tag loaded: {current_tag.name}')
                if updated_tag.template == current_tag.template:
                    tag_to_create = None
                    if not self.__tags_fields_are_equal(updated_tag, current_tag):
                        self.__synchronize_tags_fields(updated_tag, current_tag)
                        tag_to_update = current_tag

            if tag_to_create:
                tag = self.__datacatalog.create_tag(parent=entry.name, tag=tag_to_create)
                logging.info(f'Tag created: {tag.name}')
            elif tag_to_update:
                self.__datacatalog.update_tag(tag=tag_to_update, update_mask=None)
                logging.info(f'Tag updated: {tag_to_update.name}')
            else:
                logging.info(f'Tag is up to date')

    @classmethod
    def __tags_fields_are_equal(cls, tag_1, tag_2):
        for field_id in tag_1.fields:
            tag_1_field = tag_1.fields[field_id]
            tag_2_field = tag_2.fields[field_id]

            values_are_equal = tag_1_field.bool_value == tag_2_field.bool_value
            values_are_equal = values_are_equal and tag_1_field.double_value == \
                tag_2_field.double_value
            values_are_equal = values_are_equal and tag_1_field.string_value == \
                tag_2_field.string_value
            values_are_equal = values_are_equal and \
                tag_1_field.timestamp_value.seconds == \
                tag_2_field.timestamp_value.seconds
            values_are_equal = values_are_equal and \
                tag_1_field.enum_value.display_name == \
                tag_2_field.enum_value.display_name

            if not values_are_equal:
                return False

        return True

    @classmethod
    def __synchronize_tags_fields(cls, source_tag, target_tag):
        for field_id in source_tag.fields:
            source_field = source_tag.fields[field_id]
            target_field = target_tag.fields[field_id]

            if not target_field.bool_value == source_field.bool_value:
                target_field.bool_value = source_field.bool_value
            elif not target_field.double_value == source_field.double_value:
                target_field.double_value = source_field.double_value
            elif not target_field.string_value == source_field.string_value:
                target_field.string_value = source_field.string_value
            elif not target_field.timestamp_value.seconds == source_field.timestamp_value.seconds:
                target_field.timestamp_value.FromJsonString(
                    source_field.timestamp_value.ToJsonString())
            elif not target_field.enum_value.display_name == source_field.enum_value.display_name:
                target_field.enum_value.display_name = source_field.enum_value.display_name

        return target_tag
