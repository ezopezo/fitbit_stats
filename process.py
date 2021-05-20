import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
from pprint import pprint



class PlotActivityData:
    DATE = 0
    DISTANCE = 1
    FLOORS = 2
    ELEVATION = 3
    STEPS = 4
    RESTING_HEART_RATE = 5
    BASAL_METABOLIC_RATE = 6
    TOTAL_CALORIC_EXP = 7

    SEDNETARY_ACTIVITY_DIST = 8
    SEDNETARY_ACTIVITY_MIN = 9
    LIGHTLY_ACTIVITY_DIST = 10
    LIGHTLY_ACTIVITY_MIN = 11
    MODERATELY_ACTIVITY_DIST = 12
    MODERATELY_ACTIVITY_MIN = 13
    VERY_ACTIVITY_DIST = 14
    VERY_ACTIVITY_MIN = 15

    OUT_OF_RANGE_CALS = 16
    OUT_OF_RANGE_MIN = 17
    FAT_BURN_CALS = 18
    FAT_BURN_MIN = 19
    CARDIO_CALS = 20
    CARDIO_MIN = 21
    PEAK_CALS = 22
    PEAK_MIN = 23

    header = None

    def __init__(self, from_date, till_date, dataset):
        self.from_date = from_date
        self.till_date = till_date
        self.dataset = dataset
        self.data = None


    def __enter__(self):
        self.data = open(self.dataset, 'r')
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.data.close()
        return False


    def get_legend(self, column):
        if self.header is None:
            self.header = next(self.data)
        leg = [self.header.split(',')[col] for col in column]
        print(leg)
        return leg

    
    def filter_data(self, condition):
        for row in self.data:
            row = row.split(',')
            if eval(condition):
                yield row


    def get_data(self, condition, column, type_):
        data_sequence = {key: tuple() for key in column}
        dates = list()
        for row in self.filter_data(condition):
            for iteration, col in enumerate(column):
                try:
                    if self.till_date > datetime.strptime(row[self.DATE], '%d.%m.%Y') and self.from_date < datetime.strptime(row[self.DATE], '%d.%m.%Y'):
                        if type_ == 'number':
                            data_sequence[col] = data_sequence[col] + (float(row[col]),)
                        else:
                            data_sequence[col] = data_sequence[col] + (str(row[col]),)
                        if iteration == 0:
                            dates.append(datetime.strptime(row[self.DATE], '%d.%m.%Y'))
                except ValueError:  # handle N/A values later
                    print(row)
        return data_sequence, dates


    def plot_data(self, column, type_, legend_, condition='True'):
        leg = self.get_legend(column)
        data_sequence, dates = self.get_data(condition, column, type_)
        for data in data_sequence.values():
            x = mdates.date2num(dates)
            fit = np.polyfit(x, data, deg=5)
            p = np.poly1d(fit) 
            plt.plot(x,p(x),"r")
            #plt.yscale('linear')
            if legend_:
                plt.legend([plt.scatter(dates, value, s=10) for value in data_sequence.values()],
                leg, scatterpoints=1, loc='best', ncol=3, fontsize=8)
            else:
                plt.scatter(dates, data, s=10)
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d.%m.%Y'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator((1)))
        plt.gcf().autofmt_xdate()
        plt.show()


    def plot_distance(self):
        self.plot_data(column=[self.DISTANCE], type_='number', legend_=True)

    
    def plot_floors(self):
        self.plot_data(column=[self.FLOORS], type_='number', legend_=True)


    def plot_elevation(self):
        self.plot_data(column=[self.ELEVATION], type_='number', legend_=True)


    def plot_steps(self):
        self.plot_data(column=[self.STEPS], type_='number', legend_=True)


    def plot_resting_heart_rate(self):
        self.plot_data(column=[self.RESTING_HEART_RATE], type_='number', legend_=True)

    
    def plot_basal_metabolic_rate(self):
        self.plot_data(column=[self.BASAL_METABOLIC_RATE], type_='number', legend_=True)


    def plot_total_caloric_expenditure(self):
        self.plot_data(column=[self.TOTAL_CALORIC_EXP], type_='number', legend_=True)


    def plot_activity_distance(self):
        self.plot_data(column=[self.SEDNETARY_ACTIVITY_DIST, 
                                self.LIGHTLY_ACTIVITY_DIST, 
                                self.MODERATELY_ACTIVITY_DIST, 
                                self.VERY_ACTIVITY_DIST], 
                                type_='number', legend_=True)


    def plot_activity_minute(self):
        self.plot_data(column=[self.SEDNETARY_ACTIVITY_MIN, 
                                self.LIGHTLY_ACTIVITY_MIN, 
                                self.MODERATELY_ACTIVITY_MIN, 
                                self.VERY_ACTIVITY_MIN], 
                                type_='number', legend_=True)


    def plot_burned_cals_in_heart_rate_zones(self):
        self.plot_data(column=[self.OUT_OF_RANGE_CALS, 
                                self.FAT_BURN_CALS, 
                                self.CARDIO_CALS, 
                                self.PEAK_CALS], 
                                type_='number', legend_=True)


    def plot_heart_zones_minutes(self):
        self.plot_data(column=[self.OUT_OF_RANGE_MIN, 
                                self.FAT_BURN_MIN, 
                                self.CARDIO_MIN, 
                                self.PEAK_MIN], 
                                type_='number', legend_=True)


class PlotSleepData(PlotActivityData):
    DATE = 0
    RECORD_TYPE = 25
    START_TIME = 28
    END_TIME = 29

    SLEEP_LEVEL_SEQUENCE_STRING = 30
    DEEP_COUNT = 31
    DEEP_MIN = 32
    LIGHT_COUNT = 33
    LIGHT_MIN = 34
    REM_COUNT = 35
    REM_MIN = 36
    WAKE_COUNT = 37
    WAKE_MIN = 38
    MINUTES_AFTER_WAKEUP = 39
    MINUTES_ASLEEP = 40 #
    MINUTES_AWAKE = 41 #
    MINUTES_TO_FALL_ASLEEP = 42

    def __init__(self, from_date, till_date, dataset):
        self.from_date = from_date
        self.till_date = till_date
        self.dataset = dataset

    
    def plot_start_end_time(self):
        data_sequence, dates = self.get_data('row[25] == "full"', [self.START_TIME, self.END_TIME], type_='other')
        start_time_dates = mpl.dates.date2num(dates)
        end_time_dates = mpl.dates.date2num(dates)
        y = mpl.dates.date2num(dates) - mpl.dates.date2num(data_sequence[self.START_TIME])
        z = mpl.dates.date2num(dates) - mpl.dates.date2num(data_sequence[self.END_TIME])
        start_time_dates -= y
        end_time_dates -= z

        start_times = start_time_dates % 1 + int(start_time_dates[0]) 
        #end_times = end_time_dates % 1 + int(end_time_dates[0])

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot_date(start_time_dates, start_times)
        #ax.plot_date(end_time_dates, end_times)
        ax.yaxis_date()
        fig.autofmt_xdate()
        ax.yaxis.set_major_locator(mpl.dates.HourLocator()) 
        ax.yaxis.set_major_formatter(mpl.dates.DateFormatter('%H:%M'))

        plt.show()


    def parse_sleep_pattern(self, sleep_pattern):
        print([sleep_stage.split('_') for sleep_stage in sleep_pattern.split('*')])

    def plot_sleep_pattern_string(self):
        data_sequence, dates = self.get_data('row[25] == "full"', [self.SLEEP_LEVEL_SEQUENCE_STRING], type_='other')
        for sleep_pattern_collection in data_sequence.values():
            for sleep_pattern in sleep_pattern_collection:
                self.parse_sleep_pattern(sleep_pattern)


    def plot_minutes_asleep(self):
        self.plot_data(column=[self.MINUTES_ASLEEP], 
                                type_='number', condition='row[25] == "full"', legend_=True)
    

    def plot_minutes_awake(self):
        self.plot_data(column=[self.MINUTES_AWAKE], 
                                type_='number', condition='row[25] == "full"', legend_=True)


    def plot_minutes_asleep_and_awake(self):
        self.plot_data(column=[self.MINUTES_ASLEEP, 
                                self.MINUTES_AWAKE], 
                                type_='number', condition='row[25] == "full"', legend_=True)

    def plot_rem_stage_minutes(self):
        self.plot_data(column=[self.DEEP_MIN], 
                                type_='number', condition='row[25] == "full"', legend_=True)          

    def plot_sleep_stages_minutes(self):
        self.plot_data(column=[self.DEEP_MIN, 
                                self.LIGHT_MIN,
                                self.REM_MIN,
                                self.WAKE_MIN], 
                                type_='number', condition='row[25] == "full"', legend_=True)

    
    def plot_sleep_stages_count(self):
        self.plot_data(column=[self.DEEP_COUNT, 
                                self.LIGHT_COUNT,
                                self.REM_COUNT,
                                self.WAKE_COUNT], 
                                type_='number', condition='row[25] == "full"', legend_=True)


with PlotActivityData(datetime(2017, 8, 23), datetime(2021, 5, 20),'fitbit_stats.csv') as activity:
    #activity.plot_steps()
    activity.plot_distance()
    #activity.plot_resting_heart_rate()
    #activity.plot_total_caloric_expenditure()
    #activity.plot_heart_zones_minutes()
    #activity.plot_activity_minute()
    #activity.plot_basal_metabolic_rate()
    pass



with PlotSleepData(datetime(2017, 8, 23), datetime(2021, 5, 20), 'fitbit_stats.csv') as sleep:
    sleep.plot_minutes_asleep()
    '''
    Input: start date / end date / all
    Graph: all offered options
    '''
    pass




#plot_sleep.plot_sleep_pattern_string()