from python_fitbit_master import fitbit
import python_fitbit_master.gather_keys_oauth2 as Oauth2
import datetime
import os.path
import time
import csv
from pprint import pprint
from collections import namedtuple
import pandas as pd


class CollectData:
    requests_counter = 0
    cycle_counter = 0
    header = ('date', 
            'distance', 
            'floors', 
            'elevation', 
            'steps',
            'resting_heart_rate', 
            'basal_metabolic_rate',
            'total_caloric_exp',
            'sedentary_activity_dist',
            'sedentary_activity_min',
            'lightly_activity_dist', 
            'lightly_activity_min',
            'moderately_activity_dist',
            'moderately_activity_min',
            'very_activity_dist', 
            'very_activity_min',
            'out_of_range_cals',
            'out_of_range_min',
            'fat_burn_cals',
            'fat_burn_min',
            'cardio_cals',
            'cardio_min',
            'peak_cals',
            'peak_min')
        
    sleep_header = ('date',
                    'record_type',
                    'duration', 
                    'efficiency', 
                    'start_time', 
                    'end_time', 
                    'sleep_level_sequence_string',
                    'deep_count',
                    'deep_min',
                    'light_count',
                    'light_min',
                    'rem_count',
                    'rem_min',
                    'wake_count',
                    'wake_min',
                    'minutes_after_wakeup',
                    'minutes_asleep',
                    'minutes_awake',
                    'minutes_to_fall_asleep')

    def __init__(self, _file):
        self._file = _file
        

    def __enter__(self):
        # Using the ID and Secret, we can obtain the access and refresh tokens that authorize us to get our data.
        KEYS = open('keys.txt', 'r').readlines()
        self.CLIENT_ID = KEYS[0].strip('\n')
        self.CLIENT_SECRET = KEYS[1].strip('\n')
        self.server = Oauth2.OAuth2Server(self.CLIENT_ID, self.CLIENT_SECRET)
        self.server.browser_authorize()
        ACCESS_TOKEN = str(self.server.fitbit.client.session.token['access_token'])
        REFRESH_TOKEN = str(self.server.fitbit.client.session.token['refresh_token'])
        self.auth2_client = fitbit.Fitbit(self.CLIENT_ID, self.CLIENT_SECRET, oauth2=True, access_token=ACCESS_TOKEN, refresh_token=REFRESH_TOKEN) #TODO token dict
        self.auth2_client.API_VERSION = 1.2
        return self


    def __exit__(self, ex_type, ex_value, ex_traceback):
        print(ex_type, ex_value, ex_traceback)
        return False
        

    def intraday_dates(self, formated_date_to_past, last_collected_date):
        '''Produce strings of dates sequence.'''
        return [timestamp.to_pydatetime().strftime('%Y-%m-%d')
                for timestamp in reversed(pd.date_range(formated_date_to_past, last_collected_date).tolist())]
    

    def sleep_and_activity_dates(self, formated_date_to_past, last_collected_date):
        '''Produce sequence of date objects.'''
        return [timestamp.to_pydatetime()
                for timestamp in reversed(pd.date_range(formated_date_to_past, last_collected_date).tolist())]


    @classmethod
    def counter_of_requests(cls, request):
        cls.requests_counter += request
        print('Request number: ', cls.requests_counter)
        if cls.requests_counter > 139:
            print('Waiting...')
            cls.cycle_counter += 1
            print('Number of cycles: ', cls.cycle_counter)
            time.sleep(3600) # avoiding "too many requests error" - 150 requests per hour
            cls.requests_counter = 0
            print('Requests continue...')


    #### Checking files ####
    def check_last_date_in_collected_data(self, file):
        with open(file, "r") as file:
            for last_line in file:
                pass
            return datetime.datetime.strptime(last_line.split(',')[0], '%d.%m.%Y')


    def check_most_recent_date_in_collected_data(self, file):
        with open(file, "r") as file:
            next(file) # header
            return datetime.datetime.strptime(next(file).split(',')[0], '%d.%m.%Y')

    
    def read_date_to_past(self):
        if not os.path.isfile('date_to_past.txt'):
            self.request_date_to_past_for_collecting_data()
        date_to_past = open('date_to_past.txt', 'r').readline()
        return datetime.datetime.strptime(date_to_past, '%Y/%m/%d')
    ##########################


    def request_date_to_past_for_collecting_data(self):
        start_date = input('Date to past, you want collect data (format year month day): ')
        while True:
            try:
                formated_start_date = datetime.datetime(*tuple([int(num) for num in start_date.split(' ')]))
                with open('date_to_past.txt', 'w') as w_start_date:
                    w_start_date.write(formated_start_date.strftime('%Y/%m/%d'))
                    #w_start_date.write('\nDo not delete this file while collecting data! Date may be edited but be aware of correct format.')
                break
            except Exception as ex:
                print(ex)
                start_date = input('Date to past, you want collect data (format year month day): ')
                break


    def fill_temporary_csv(self):
        if not os.path.isfile('fitbit_data_temp.csv'):
            self.write_data_to_csv('fitbit_data_temp.csv', 'w', header=True)
        else:
            most_recent_date = self.check_most_recent_date_in_collected_data(self._file)
            last_collected_date = self.check_last_date_in_collected_data('fitbit_data_temp.csv')
            self.intraday_dates_range = self.intraday_dates(most_recent_date, 
                                                            last_collected_date)[1:-1]
            self.sleep_and_activity_dates_range = self.sleep_and_activity_dates(most_recent_date, 
                                                                                last_collected_date)[1:-1]
            self.write_data_to_csv('fitbit_data_temp.csv', 'a', header=False)


    def merge_files(self):
        # check if last date in temp_file is one day bigger than first date in self._file
        print('merging')
        recent_date = self.check_most_recent_date_in_collected_data(self._file)
        last_tmp_date = self.check_last_date_in_collected_data('fitbit_data_temp.csv')
        recent_date = recent_date + datetime.timedelta(days=1)
        print(recent_date, last_tmp_date)
        # create new csv file (updated_file) with underscore difference from self._file
        updated_file = str(self._file).replace('.', '_.')
        if recent_date == last_tmp_date:
            print('________________')
            with open(updated_file, 'w') as all_data_file:
                # fill data from temp_file to updated_file
                with open('fitbit_data_temp.csv', 'r') as new_data:
                    for line in new_data:
                        all_data_file.write(line)
                
                # fill data from self._file to updated_file
                with open(self._file, 'r') as old_data:
                    for line in old_data:
                        if not line[:4] == 'date':
                            all_data_file.write(line)
                # remove self._file
                os.remove(self._file)
                # rename updated_file to self._file
            os.rename(updated_file, self._file)
            print('__________________')
            exit()
        else:
            print('Temp file needs update... After updating finishes restart script please.')
            self.fill_temporary_csv()


    def delete_temporary_file(self):
        if os.path.isfile('fitbit_data_temp.csv'):
            print('Deleting temporary file...')
            #os.remove('fitbit_data_temp.csv')

    
    def control_data_collection_to_past(self, formated_date_to_past, last_collected_date):
        if formated_date_to_past < last_collected_date:
            print('Continuing collecting data...')
            # timerange
            self.intraday_dates_range = self.intraday_dates(formated_date_to_past, 
                                                            last_collected_date)[1:]
            self.sleep_and_activity_dates_range = self.sleep_and_activity_dates(formated_date_to_past, 
                                                                                last_collected_date)[1:]
            print(self.intraday_dates_range, self.sleep_and_activity_dates_range)
            self.write_data_to_csv(self._file, 'a', header=False)
        elif formated_date_to_past == last_collected_date:
            print('Data are completely collected to specified date. Checking updating data...')
        else:
            print('Error last date in collected data is older than specified date. Change date in date_to_past.txt')


    def control_data_updating(self, most_recent_date):
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        if most_recent_date < yesterday:
            print('Updating data. Last collected date: ', most_recent_date)
            self.intraday_dates_range = self.intraday_dates(most_recent_date, 
                                                                yesterday)[:-1]
            self.sleep_and_activity_dates_range = self.sleep_and_activity_dates(most_recent_date, 
                                                                                yesterday)[:-1]

            #print(self.intraday_dates_range, self.sleep_and_activity_dates_range)
            self.fill_temporary_csv()
            self.merge_files()
            self.delete_temporary_file()
            # create temporary csv, fill data, merge files (lazily), delete temporary file
        elif most_recent_date == yesterday:
            print('Data are updated to latest (yesterday). Use process.py for visualization.')
        else:
            print("Error, most recent date in csv doesn't make sense.")


    def collection_control_node(self):
        formated_date_to_past = self.read_date_to_past()
        print('Collecting data to date to past specified: ', str(formated_date_to_past))
        if not os.path.isfile(self._file):
            print('Creating data collection csv file named ', self._file)
            f = open(self._file, mode='w') # just create it
            f.close()

        start_data_mining = input('Do you want to start collecting data? [y/n] ')
        if start_data_mining == 'y':
            # start collecting - appending
            last_collected_date = self.check_last_date_in_collected_data(self._file)
            self.control_data_collection_to_past(formated_date_to_past, last_collected_date)
            # inserting (or updating)
            most_recent_date = self.check_most_recent_date_in_collected_data(self._file)
            self.control_data_updating(most_recent_date)
        else:
            exit()




    ### Data methods ####
    def collect_movement(self, stats):
        movement_attributes = ('distance', 'floors', 'elevation', 'steps')
        movement_values = list()
        for movement_attribute in movement_attributes:
            try:
                movement_values.append(stats['summary'][movement_attribute])
            except KeyError:
                movement_values.append('N/A')

        return movement_values
    
    
    def resting_heart_rate(self, hr_stats):
        try:
            return hr_stats['activities-heart'][0]['value']['restingHeartRate']
        except KeyError:
            return 'N/A'


    def collect_calories(self, stats):
        try:
            return stats['summary']['calories']['bmr'], stats['summary']['calories']['total']
        except KeyError:
            return 'N/A', 'N/A'

    
    def flatten(self, sequence):
        return [element for subseq in sequence for element in subseq]


    ##### activity stats #####
    def collect_all_levels_of_activity(self, activity):
        activity_levels = ('sedentary', 'lightly', 'moderately', 'very')
        activity_levels_values = list()
        for activity_level in activity_levels:
            try:
                activity_levels_values.append(activity[activity_level])
            except KeyError:
                activity_levels_values.append(['N/A', 'N/A'])

        activity_levels_values = self.flatten(activity_levels_values)
        return activity_levels_values


    def collect_activity_levels(self, stats):
        try:
            activity = {stats['summary']['activityLevels'][i]['name']:
                [stats['summary']['activityLevels'][i]['distance'], 
                stats['summary']['activityLevels'][i]['minutes']] 
                for i in range(len(stats['summary']['activityLevels']))}
        except KeyError:
            return ('N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A')

        return self.collect_all_levels_of_activity(activity)
                    

    ##### HR zones #####
    def yield_hr_zones(self, hr_zones):
        zones = ('Out of Range', 'Fat Burn', 'Cardio', 'Peak')
        zones_values = list()
        for zone in zones:
            try:
                zones_values.append(hr_zones[zone])
            except KeyError:
                zones_values.append(['N/A', 'N/A'])

        zones_values = self.flatten(zones_values)
        return zones_values
        

    def collect_heart_rate_zones(self, stats):
        try:
            hr_zones = {stats['summary']['heartRateZones'][i]['name']: 
                [stats['summary']['heartRateZones'][i]['caloriesOut'], 
                stats['summary']['heartRateZones'][i]['minutes']] 
                for i in range(len(stats['summary']['heartRateZones']))}
            return self.yield_hr_zones(hr_zones)
        except KeyError:
            return ('N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A')
    ####################


    def activity_stats(self):
        ActivityData = namedtuple('ActivityData', self.header)
        for sleep_activity_date, intraday_date in zip(self.sleep_and_activity_dates_range, 
                                                        self.intraday_dates_range):
            stats = self.auth2_client.activities(sleep_activity_date)
            hr_stats = self.auth2_client.intraday_time_series('activities/heart', 
                                                                    base_date=intraday_date, 
                                                                    detail_level='1sec')

            date = sleep_activity_date.strftime('%d.%m.%Y')
            distance, \
            floors, \
            elevation, \
            steps = self.collect_movement(stats)
            resting_heart_rate = self.resting_heart_rate(hr_stats)
            basal_metabolic_rate, \
            total_caloric_exp = self.collect_calories(stats)
            sedentary_activity_dist, \
            sedentary_activity_min, \
            lightly_activity_dist, \
            lightly_activity_min, \
            moderately_activity_dist, \
            moderately_activity_min, \
            very_activity_dist, \
            very_activity_min = self.collect_activity_levels(stats)
            out_of_range_cals, \
            out_of_range_min, \
            fat_burn_cals, \
            fat_burn_min, \
            cardio_cals, \
            cardio_min, \
            peak_cals, \
            peak_min = self.collect_heart_rate_zones(stats)

            self.counter_of_requests(1)
            yield ActivityData(date, 
                    distance, 
                    floors, 
                    elevation, 
                    steps,
                    resting_heart_rate,
                    basal_metabolic_rate, 
                    total_caloric_exp, 
                    sedentary_activity_dist,
                    sedentary_activity_min,
                    lightly_activity_dist,
                    lightly_activity_min,
                    moderately_activity_dist,
                    moderately_activity_min,
                    very_activity_dist,
                    very_activity_min, 
                    out_of_range_cals,
                    out_of_range_min,
                    fat_burn_cals,
                    fat_burn_min,
                    cardio_cals,
                    cardio_min,
                    peak_cals,
                    peak_min)


    ##### Sleep data #####
    def start_end_time_of_sleep(self, stats):
        try:
            return stats['sleep'][0]['startTime'], stats['sleep'][0]['endTime']
        except KeyError:
            return 'N/A', 'N/A'


    def parse_sleep_pattern(self, stats):
        full_record = ''
        for record in stats['sleep'][0]['levels']['data']:
            full_record = full_record + '*' + str(record['dateTime']) + '_' + str(record['level']) + '_' + str(record['seconds'])
        return full_record[1:]


    def obtain_sleep_level_count(self, stats, sleep_level):
        try:
            return stats['sleep'][0]['levels']['summary'][sleep_level]['count']
        except KeyError:
            return 'N/A'

    
    def obtain_sleep_level_minutes(self, stats, sleep_level):
        try:
            return stats['sleep'][0]['levels']['summary'][sleep_level]['minutes']
        except KeyError:
            return 'N/A'


    def summary_sleep(self, stats):
        sleep_levels = ('deep', 'light', 'rem', 'wake')
        summary_sleep_levels = tuple()
        for sleep_level in sleep_levels:
            summary_sleep_levels = summary_sleep_levels + (self.obtain_sleep_level_count(stats, sleep_level),
                                                            self.obtain_sleep_level_minutes(stats, sleep_level))
        return summary_sleep_levels

    
    def collect_sleep_attributes(self, stats):
        sleep_attributes = ('duration', 'efficiency', 'minutesAfterWakeup', 
                            'minutesAsleep', 'minutesAwake', 'minutesToFallAsleep')
        sleep_attributes_data = list()
        for sleep_attribute in sleep_attributes:
            try:
                sleep_attributes_data.append(stats['sleep'][0][sleep_attribute])
            except KeyError:
                sleep_attributes_data.append('N/A')
        
        return sleep_attributes_data
    ####################


    def sleep_stats(self):
        SleepData = namedtuple('SleepData', self.sleep_header)
        for day in self.sleep_and_activity_dates_range:
            stats = self.auth2_client.sleep(date=day)
            try:
                if stats['sleep'][0]['isMainSleep'] and stats['sleep'][0]['type'] == 'stages': # full record
                    date = day
                    record_type = 'full'
                    start_time, end_time = self.start_end_time_of_sleep(stats)
                    sleep_level_sequence_string = self.parse_sleep_pattern(stats)
                    deep_count, \
                    deep_min, \
                    light_count, \
                    light_min, \
                    rem_count, \
                    rem_min, \
                    wake_count, \
                    wake_min = self.summary_sleep(stats)
                    duration, \
                    efficiency, \
                    minutes_after_wakeup, \
                    minutes_asleep, \
                    minutes_awake, \
                    minutes_to_fall_asleep = self.collect_sleep_attributes(stats)
                elif stats['sleep'][0]['isMainSleep'] and stats['sleep'][0]['type'] == 'classic': # partial record
                    date = day
                    record_type = 'partial'
                    start_time, end_time = self.start_end_time_of_sleep(stats)
                    sleep_level_sequence_string = self.parse_sleep_pattern(stats)
                    deep_count = 'N/A'
                    deep_min = 'N/A'
                    light_count = 'N/A'
                    light_min = 'N/A'
                    rem_count = 'N/A'
                    rem_min = 'N/A'
                    wake_count = 'N/A'
                    wake_min = 'N/A'
                    duration, \
                    efficiency, \
                    minutes_after_wakeup, \
                    minutes_asleep, \
                    minutes_awake, \
                    minutes_to_fall_asleep = self.collect_sleep_attributes(stats)
            except IndexError:
                date = day
                record_type = 'none'
                start_time, end_time, sleep_level_sequence_string, deep_count, \
                deep_min, light_count, light_min, rem_count, rem_min, \
                wake_count, wake_min, duration, efficiency, \
                minutes_after_wakeup, minutes_asleep, minutes_awake, \
                minutes_to_fall_asleep = ['N/A' for _ in range(17)]

            self.counter_of_requests(1)
            yield SleepData(date,
                    record_type,
                    duration, 
                    efficiency, 
                    start_time, 
                    end_time, 
                    sleep_level_sequence_string,
                    deep_count,
                    deep_min,
                    light_count,
                    light_min,
                    rem_count,
                    rem_min,
                    wake_count,
                    wake_min,
                    minutes_after_wakeup,
                    minutes_asleep,
                    minutes_awake,
                    minutes_to_fall_asleep)


    def write_data_to_csv(self, file, mode='a', header=False):
        with open(file, mode, newline='') as csvfile:
            write = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            if header:
                write.writerow(self.header + self.sleep_header)
            for activity_data, sleep_data in zip(self.activity_stats(), self.sleep_stats()):
                print(activity_data)
                print(sleep_data)
                write.writerow(tuple(activity_data) + tuple(sleep_data)) # convert to normal tuple


    def wholetime_stats(self):
        pprint(self.auth2_client.frequent_activities())
        pprint(self.auth2_client.favorite_activities())



if __name__ == '__main__':
    with CollectData('fitbit_stats_5.csv') as col:
       col.collection_control_node()

    