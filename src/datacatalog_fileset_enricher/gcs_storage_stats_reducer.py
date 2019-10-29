class GCStorageStatsReducer:

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
                'created_files_by_day': cls.__get_daily_stats(time_created, 'time_created'),
                'updated_files_by_day': cls.__get_daily_stats(time_updated, 'time_updated'),
                'prefix': prefix
            }
        else:
            stats = {
                'count': 0,
                'prefix': prefix
            }

        return stats

    @classmethod
    def __get_daily_stats(cls, series, timestamp_column):
        time_created_same_day = series.apply(lambda timestamp: timestamp._date_repr).to_frame()
        value = ''
        for day, count in time_created_same_day[timestamp_column].value_counts().iteritems():
            value += f'{day} [count: {count}], '
        return value[:-2]
