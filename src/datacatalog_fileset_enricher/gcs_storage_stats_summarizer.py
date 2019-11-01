class GCStorageStatsSummarizer:

    @classmethod
    def create_stats_from_dataframe(cls, dataframe, prefix, filtered_buckets_stats,
                                    execution_time, bucket_prefix):
        if dataframe is not None:
            size = dataframe['size']
            time_created = dataframe['time_created']
            time_updated = dataframe['time_updated']
            stats = {
                'count': len(dataframe),
                'min_size': size.min(),
                'max_size': size.max(),
                'avg_size': size.mean(),
                'total_size': size.sum(),
                'min_created': time_created.min(),
                'max_created': time_created.max(),
                'min_updated': time_updated.min(),
                'max_updated': time_updated.max(),
                'created_files_by_day': cls.__get_daily_stats(time_created, 'time_created'),
                'updated_files_by_day': cls.__get_daily_stats(time_updated, 'time_updated'),
                'prefix': prefix,
                'files_by_bucket': cls.__get_files_by_bucket(filtered_buckets_stats),
                'files_by_type': cls.__get_files_by_type(dataframe),
                'buckets_found': len(filtered_buckets_stats),
                'execution_time': execution_time,
                'bucket_prefix': bucket_prefix
            }
        else:
            buckets_found = 0
            for bucket_stats in filtered_buckets_stats:
                # This placeholder controls if the prefix was created with a non existent bucket
                bucket_not_found = bucket_stats.get('bucket_not_found')
                if not bucket_not_found:
                    buckets_found += 1

            stats = {
                'count': 0,
                'prefix': prefix,
                'files_by_bucket': cls.__get_files_by_bucket(filtered_buckets_stats),
                'buckets_found': buckets_found,
                'execution_time': execution_time,
                'bucket_prefix': bucket_prefix
            }

        return stats

    @classmethod
    def __get_daily_stats(cls, series, timestamp_column):
        time_created_same_day = series.apply(lambda timestamp: timestamp._date_repr).to_frame()
        value = ''
        for day, count in time_created_same_day[timestamp_column].value_counts().iteritems():
            value += f'{day} [count: {count}], '
        return value[:-2]

    @classmethod
    def __get_files_by_bucket(cls, filtered_buckets_stats):
        value = ''
        for bucket_stats in filtered_buckets_stats:
            value += f'{bucket_stats["bucket_name"]} [count: {bucket_stats["files"]}], '
        return value[:-2]

    @classmethod
    def __get_files_by_type(cls, dataframe):
        series = dataframe['name']
        files_types = series.apply(cls.__extract_file_type).to_frame()
        value = ''
        for file_type, count in files_types['name'].value_counts().iteritems():
            value += f'{file_type} [count: {count}], '
        return value[:-2]

    @classmethod
    def __extract_file_type(cls, file_name):
        file_type_at = file_name.rfind('.')
        if file_type_at != -1:
            return file_name[file_type_at+1:]
        else:
            return 'unknown_file_type'
