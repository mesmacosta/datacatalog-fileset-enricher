class GCStorageStatsSummarizer:

    @classmethod
    def create_stats_from_dataframe(cls, dataframe, file_patterns, filtered_buckets_stats,
                                    execution_time, bucket_prefix):

        buckets_found, files_by_bucket = cls.__process_bucket_stats(filtered_buckets_stats)

        if dataframe is not None:
            size = dataframe['size']
            time_created = dataframe['time_created']
            time_updated = dataframe['time_updated']
            stats = {
                'count': len(dataframe),
                'min_size': cls.__convert_to_mb(size.min()),
                'max_size': cls.__convert_to_mb(size.max()),
                'avg_size': cls.__convert_to_mb(size.mean()),
                'total_size': cls.__convert_to_mb(size.sum()),
                'min_created': time_created.min(),
                'max_created': time_created.max(),
                'min_updated': time_updated.min(),
                'max_updated': time_updated.max(),
                'created_files_by_day': cls.__get_daily_stats(time_created, 'time_created'),
                'updated_files_by_day': cls.__get_daily_stats(time_updated, 'time_updated'),
                'prefix': cls.__get_prefix(file_patterns),
                'files_by_bucket': files_by_bucket,
                'files_by_type': cls.__get_files_by_type(dataframe),
                'buckets_found': buckets_found,
                'execution_time': execution_time,
                'bucket_prefix': bucket_prefix
            }
        else:
            stats = {
                'count': 0,
                'prefix': cls.__get_prefix(file_patterns),
                'files_by_bucket': files_by_bucket,
                'buckets_found': buckets_found,
                'execution_time': execution_time,
                'bucket_prefix': bucket_prefix
            }

        return stats

    @classmethod
    def __convert_to_mb(cls, size_bytes, round_cases=2):
        return float(f'{(size_bytes / 1000 / 1000):.{round_cases}f}')

    @classmethod
    def __get_daily_stats(cls, series, timestamp_column):
        time_created_same_day = series.apply(lambda timestamp: timestamp._date_repr).to_frame()
        value = ''
        for day, count in time_created_same_day[timestamp_column].value_counts().iteritems():
            value += f'{day} [count: {count}], '
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
    def __get_prefix(cls, file_patterns):
        value = ''
        for file_pattern in file_patterns:
            value += f'{file_pattern}, '
        return value[:-2]

    @classmethod
    def __extract_file_type(cls, file_name):
        file_type_at = file_name.rfind('.')
        if file_type_at != -1:
            return file_name[file_type_at + 1:]
        else:
            return 'unknown_file_type'

    @classmethod
    def __process_bucket_stats(cls, filtered_buckets_stats):
        processed_bucket_stats_dict = {}

        # Consolidate repeated buckets in case we have more than one file_pattern
        for bucket_stats in filtered_buckets_stats:
            bucket_not_found = bucket_stats.get('bucket_not_found')
            if not bucket_not_found:
                bucket_name = bucket_stats['bucket_name']
                bucket_files_sum = processed_bucket_stats_dict.get(bucket_name)
                bucket_files_count = bucket_stats['files']
                if not bucket_files_sum:
                    bucket_files_sum = 0

                bucket_files_sum += bucket_files_count

                processed_bucket_stats_dict[bucket_name] = bucket_files_sum

        files_by_bucket = ''
        for bucket_name, files_sum in processed_bucket_stats_dict.items():
            files_by_bucket += f'{bucket_name} [count: {files_sum}], '

        files_by_bucket = files_by_bucket[:-2]

        if not files_by_bucket:
            files_by_bucket = 'bucket_not_found'

        buckets_found = len(processed_bucket_stats_dict.keys())
        return buckets_found, files_by_bucket
