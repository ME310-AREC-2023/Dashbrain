from influxdb_client import InfluxDBClient
from datetime import datetime
from dateutil import tz
import pandas as pd
import os
import matplotlib
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'
from sqlalchemy import create_engine
# dependent on pymysql
# if not os.path.exists('alerts.csv'):
iniAlerts = {
    'type': [],
    'location': [],
    'time': [],
    'duration': [],
    'severity': [],
    'active': []
}
iniAlerts = pd.DataFrame(iniAlerts)
iniAlerts.to_csv('alerts.csv', index=False)

# iniIndexes = {
#     'location': [],
#     'time': [],
#     'atime': [],
#     'HIndex': [],
#     'ECO2': [],
#     'temp': [],
#     'humidity': [],
#     'bVOC': [],
# }
# iniIndexes = pd.DataFrame(iniIndexes)
# iniIndexes.to_csv('indexes.csv', index=False)


# pulls from given timestamp forward eg. '2023-05-17 05:34:44.592883+00:00'
def inPull(start):
    client = InfluxDBClient(
        url='http://192.168.10.105:8086',  # replace with your InfluxDB URL
        token='5AX7j_ZBLik0ktCVln6nFNFybEqT12F9oE7bMJrC_YNfS4Cd-hxuecyoUh6dQz9D97o-vQYgaVmgusV3UcFA0A==',
        # replace with your token
        org='AREC 22-23'  # replace with your organization
    )
    start_time_str = start
    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S.%f%z')
    f_start_time = start_time.astimezone(tz.UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # Create a query API
    query_api = client.query_api()

    # Define your Flux query

    # Define your Flux query
    query = f'''from(bucket: "SensorBox-test")
            |> range(start: {f_start_time})
            |> filter(fn: (r) => r._measurement == "SensorBox")
            |> yield()
    '''

    # Execute the query
    current_time = datetime.now()
    print('pulling started at '+str(current_time)+'. . .')
    result = query_api.query(query)
    # Print the result
    data = []
    for table in result:
        for record in table.records:
            data.append(record.values)

    # Create a DataFrame
    df = pd.DataFrame(data)

    # Save DataFrame to a CSV file
    df.to_csv('ind.csv', index=False)



def fetch_data(csv_file):
    # Read CSV data
    raw_data = pd.read_csv(csv_file)

    # Filter the data based on sensor_id and _field column
    new_data = raw_data[(raw_data['_field'] == 'eCO2')]

    # Convert timestamp to epoch and multiply _value by 1000
    new_data['_time'] = pd.to_datetime(new_data['_time'], format='ISO8601')
    new_data.rename(columns={'_time': 'time'}, inplace=True)
    new_data.rename(columns={'_value': 'eCO2'}, inplace=True)
    new_data.reset_index(inplace=True)

    # Filter the data based on sensor_id and _field column for temperature
    new_temp_data = raw_data[(raw_data['_field'] == 'temp')]

    # Convert timestamp to epoch
    new_temp_data['_time'] = pd.to_datetime(new_temp_data['_time'], format='ISO8601')
    new_temp_data.rename(columns={'_value': 'temp'}, inplace=True)
    new_temp_data.reset_index(inplace=True)

    new_humid_data = raw_data[(raw_data['_field'] == 'humidity')]
    new_humid_data['_time'] = pd.to_datetime(new_humid_data['_time'], format='ISO8601')
    new_humid_data.rename(columns={'_value': 'humidity'}, inplace=True)
    new_humid_data.reset_index(inplace=True)

    new_iaq_data = raw_data[(raw_data['_field'] == 'iaq')]
    new_iaq_data['_time'] = pd.to_datetime(new_iaq_data['_time'], format='ISO8601')
    new_iaq_data.rename(columns={'_value': 'iaq'}, inplace=True)
    new_iaq_data.reset_index(inplace=True)

    new_bvoc_data = raw_data[(raw_data['_field'] == 'bVOC')]
    new_bvoc_data['_time'] = pd.to_datetime(new_bvoc_data['_time'], format='ISO8601')
    new_bvoc_data.rename(columns={'_value': 'bVOC'}, inplace=True)
    new_bvoc_data.reset_index(inplace=True)

    new_pre_data = raw_data[(raw_data['_field'] == 'pressure')]
    new_pre_data['_time'] = pd.to_datetime(new_pre_data['_time'], format='ISO8601')
    new_pre_data.rename(columns={'_value': 'pressure'}, inplace=True)
    new_pre_data.reset_index(inplace=True)

    # rename and append columns

    new_csv = pd.concat(
        [new_data, new_temp_data['temp'], new_humid_data['humidity'], new_iaq_data['iaq'], new_bvoc_data['bVOC'],
         new_pre_data['pressure']], axis=1)

    new_csv = new_csv.reset_index(drop=True)

    if os.path.exists('new_inf_file.csv'):
        # If data file exists, add new data
        print('appending data')
        new_csv.to_csv('new_inf_file.csv', mode='a', index=False, header=False)
    else:
        # File doesn't exist, generate file
        new_csv.to_csv('new_inf_file.csv', index=False)




class Sensor:
    def __init__(self, location):
        self.new_data = None
        # keeps track of which fields contributed to an alert
        self.temp_count = [0, 0, 0, 0]
        # keeps track of the values and durations associated with each field
        self.count = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        # keeps track of the start time of an alert
        self.tstamp = [pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00')]
        self.tcount = 0
        self.location = location
        # string containing fields that contributed to alert. printed and pushed to sql
        self.stat = ''
        # lists containing limits for each value. [max, min, target] for temp and humidity, [max, max] for the rest
        self.clim = [1100, 1100]
        self.tlim = [28, 16, 22]
        self.hlim = [50, 30, 40]
        self.blim = [2.2, 2.2]
        self.alim = [110, 110]
        self.ilim = [0, 0]
        self.tdelta = 3
        self.index = 0
        # tracks whether alert is active
        self.aindex = 0
        # variable for storing new row of indexes
        # self.indexes = {
        #     'location': [],
        #     'time': [],
        #     'atime': [],
        #     'HIndex': [],
        #     'ECO2': [],
        #     'temp': [],
        #     'humidity': [],
        #     'bVOC': [],
        # }
        # self.indexes = pd.DataFrame(self.indexes)
        # variable for storing new row of alerts
        self.alerts = {
            'type': [],
            'location': [],
            'time': [],
            'duration': [],
            'severity': [],
            'active': []
        }
        self.alerts = pd.DataFrame(self.alerts)
        print(self.location)
    # rests indexes
    def setData(self):
        self.indexes = {
            'location': [],
            'time': [],
            'atime': [],
            'HIndex': [],
            'ECO2': [],
            'temp': [],
            'humidity': [],
            'bVOC': [],
        }
        self.indexes = pd.DataFrame(self.indexes)

    def pushAlert(self, type, location, time, duration, severity, active):
        if duration == 0:
            return
        severity = 100/(1+np.exp(-np.log10(severity/1000)))
        print(type + ' alert of severity ' + str(severity) + ' at sensor ' + location + ' & time '
              + str(time) + ' for ' + str(duration) + ' seconds')
        self.alerts = pd.read_csv('alerts.csv')
        self.alerts['time'] = pd.to_datetime(self.alerts['time'], format='ISO8601')
        conditions = (self.alerts['time'] == time) & (self.alerts['type'] == type) & (
                self.alerts['location'] == location)
        alert = self.alerts.loc[conditions]
        atemp = {
            'type': [type],
            'location': [location],
            'time': [time],
            'duration': [duration],
            'severity': [severity],
            'active': [active]
        }
        atemp = pd.DataFrame(atemp)
        if not alert.empty:
            print('replaced ' + str(alert.index[0]))
            self.alerts.loc[alert.index[0]] = atemp.loc[0]
            self.alerts.to_csv('alerts.csv', index=False)
        else:
            atemp.to_csv('alerts.csv', mode='a', index=False, header=False)
        self.alerts = pd.read_csv('alerts.csv')
        self.alerts['time'] = pd.to_datetime(self.alerts['time'], format='ISO8601')

    def healthIndex(self, c, t, h, b, a, timestamp, atimestamp):
        csum = 0
        tsum = 0
        hsum = 0
        bsum = 0
        asum = 0

        if self.count[1] != 0:
            csum = (self.count[0] / self.count[1]) / self.clim[0]
            self.temp_count[0] = 1
        if self.count[3] != 0:
            tsum = (self.count[2] / self.count[3]) / self.tlim[0]
            self.temp_count[1] = 1
        if self.count[5] != 0:
            hsum = (self.count[4] / self.count[5]) / self.hlim[0]
            self.temp_count[2] = 1
        if self.count[7] != 0:
            bsum = (self.count[6] / self.count[7]) / self.blim[0]
            self.temp_count[3] = 1
        if self.count[9] != 0:
            asum = (self.count[8] / self.count[9]) / self.alim[0]


        # csum = (c) / self.clim[0]
        # tsum = (t) / (self.tlim[0]-self.tlim[2])
        # hsum = (h) / (self.hlim[0]-self.tlim[2])
        # bsum = (b) / self.blim[0]
        # asum = (a) / self.alim[0]

        self.index = csum + tsum + hsum + bsum + asum
        self.index = self.index / .05
        # itemp = {
        #     'location': [self.location],
        #     'time': [timestamp],
        #     'atime': [atimestamp],
        #     'HIndex': [self.index],
        #     'ECO2': [c],
        #     'temp': [t],
        #     'humidity': [h],
        #     'bVOC': [b],
        # }
        # itemp = pd.DataFrame(itemp)
        # self.indexes = pd.concat([self.indexes, itemp], ignore_index=True)

        sc = ''
        st = ''
        sh = ''
        sb = ''

        if self.temp_count[0] == 1:
            sc = 'ECO2 '
        if self.temp_count[1] == 1:
            st = 'Temp '
        if self.temp_count[2] == 1:
            sh = 'RH '
        if self.temp_count[3] == 1:
            sb = 'BVOC'

        self.stat = sc+st+sh+sb

    def scan(self, data):
        self.new_data = data
        self.new_data.reset_index(inplace=True)
        j = self.location
        for i in range(self.tcount, len(self.new_data)):

            # Search for co alerts
            if (self.new_data['eCO2'][i] > self.clim[0]):
                if (self.count[0] == 0):
                    self.tstamp[0] = self.new_data['time'][i]

                self.count[0] += (self.new_data['eCO2'][i] - self.clim[0])
                self.count[1] = int((self.new_data['time'][i] - self.tstamp[0]).total_seconds())


            else:
                # if (self.count[0] > self.clim[0]):
                #     self.pushAlert('eCO2', j, self.tstamp[0], self.count[1], self.count[0], 0)
                self.count[0] = 0
                self.count[1] = 0

            # Search for temperature alerts
            if (self.new_data['temp'][i] > self.tlim[0]):
                if (self.count[2] == 0):
                    self.tstamp[1] = self.new_data['time'][i]
                self.count[2] += (self.new_data['temp'][i] - self.tlim[0])
                self.count[3] = int((self.new_data['time'][i] - self.tstamp[1]).total_seconds())

            elif (self.new_data['temp'][i] < self.tlim[1]):
                if (self.count[2] == 0):
                    self.tstamp[1] = self.new_data['time'][i]
                self.count[2] += (self.tlim[1] - self.new_data['temp'][i])
                self.count[3] = int((self.new_data['time'][i] - self.tstamp[1]).total_seconds())

            else:
                # if (self.count[2] > self.tlim[2]):
                #     self.pushAlert('temp', j, self.tstamp[1], self.count[3], self.count[2], 0)
                self.count[2] = 0
                self.count[3] = 0

            # Search for humidity alerts
            if (self.new_data['humidity'][i] > self.hlim[0]):
                if (self.count[4] == 0):
                    self.tstamp[2] = self.new_data['time'][i]
                self.count[4] += (self.new_data['humidity'][i] - self.hlim[0])
                self.count[5] = int((self.new_data['time'][i] - self.tstamp[2]).total_seconds())

            elif (self.new_data['humidity'][i] < self.hlim[1]):
                if (self.count[4] == 0):
                    self.tstamp[2] = self.new_data['time'][i]
                self.count[4] += (self.hlim[0] - self.new_data['humidity'][i])
                self.count[5] = int((self.new_data['time'][i] - self.tstamp[2]).total_seconds())

            else:
                # if (self.count[4] > self.hlim[2]):
                #     self.pushAlert('humidity', j, self.tstamp[2], self.count[5], self.count[4], 0)

                self.count[4] = 0
                self.count[5] = 0

            # Search for bVOC alerts
            if (self.new_data['bVOC'][i] > self.blim[0]):
                if (self.count[6] == 0):
                    self.tstamp[3] = self.new_data['time'][i]
                self.count[6] += (self.new_data['bVOC'][i] - self.blim[0])
                self.count[7] = int((self.new_data['time'][i] - self.tstamp[3]).total_seconds())


            else:
                # if (self.count[6] > self.blim[0]):
                #     self.pushAlert('bVOC', j, self.tstamp[3], self.count[7], self.count[6], 0)

                self.count[6] = 0
                self.count[7] = 0

            # Search for iaq alerts
            if (self.new_data['iaq'][i] > self.alim[0]):
                if (self.count[8] == 0):
                    self.tstamp[4] = self.new_data['time'][i]
                self.count[8] += (self.new_data['iaq'][i] - self.alim[0])
                self.count[9] = int((self.new_data['time'][i] - self.tstamp[4]).total_seconds())

            else:
                # if (self.count[8] > self.alim[0]):
                #     self.pushAlert('iaq', j, self.tstamp[4], self.count[9], self.count[8], 0)
                self.count[9] = self.count[8]
                self.count[8] = 0


            # Search for health index alerts
            if ((i % 10) == 0):
                self.healthIndex(self.new_data['eCO2'][i], abs(self.new_data['temp'][i]-self.tlim[2]), abs(self.new_data['humidity'][i]-self.hlim[2]), self.new_data['bVOC'][i], self.new_data['iaq'][i],self.new_data['time'][i], self.tstamp[5])
                if (self.index > self.ilim[0]):
                    if (self.count[10] == 0):
                        self.tstamp[5] = self.new_data['time'][i]
                        self.aindex = 1
                    self.count[10] += (self.index - self.ilim[0])
                    self.count[11] = int((self.new_data['time'][i] - self.tstamp[5]).total_seconds())
                else:
                    if (self.aindex == 1):
                        self.pushAlert(self.stat, j, self.tstamp[5], self.count[11], self.count[10], 0)
                    self.count[10] = 0
                    self.count[11] = 0
                    self.temp_count = [0, 0, 0, 0]
                    self.tstamp[5] = pd.Timedelta('0 days 00:00:00')
                    self.aindex = 0

        if (self.tcount != len(self.new_data)):
            # if (self.count[0] > self.clim[0]):
            #     self.pushAlert('eCO2', j, self.tstamp[0], self.count[1], self.count[0], 0)
            # if (self.count[2] > self.tlim[0]):
            #     self.pushAlert('temp', j, self.tstamp[1], self.count[3], self.count[2], 0)
            # if (self.count[4] > self.hlim[0]):
            #     self.pushAlert('humidity', j, self.tstamp[2], self.count[5], self.count[4], 0)
            # if (self.count[6] > self.blim[0]):
            #     self.pushAlert('bVOC', j, self.tstamp[3], self.count[7], self.count[6], 0)
            # if (self.count[8] > self.alim[0]):
            #     self.pushAlert('iaq', j, self.tstamp[4], self.count[9], self.count[8], 0)
            if (self.aindex == 1):
                self.pushAlert(self.stat, j, self.tstamp[5], self.count[11], self.count[10], 1)
            self.tcount = len(self.new_data)
        # self.indexes.to_csv('indexes.csv', mode='a', index=False, header=False)
        # self.setData()


def sqlPush():
    engine = create_engine('mysql+pymysql://alertGenerator:ME310arec@192.168.10.103:3306/alert-test')

    alerts = pd.read_csv("alerts.csv")
    alerts['time'] = pd.to_datetime(alerts['time'], format='ISO8601')
    # alerts['duration'] = pd.to_timedelta(alerts['duration'])

    alerts.to_sql('AlertList', con=engine, if_exists='replace', index=False)

    # indexes = pd.read_csv("indexes.csv")
    # indexes['time'] = pd.to_datetime(indexes['time'], format='ISO8601')
    # indexes.to_sql('HealthIndex', con=engine, if_exists='replace', index=False)



# Limit vectors for co temp humid [limit, sum limit]

sensors = {}

# Iterate through sensors

# Call function
#while True:
for i in range(10):
    ini_time = datetime.now()
    if os.path.exists('new_inf_file.csv'):
        stgen = pd.read_csv('new_inf_file.csv')
        # Perform your operations here
        start_time = stgen['time'].iloc[-1]
        print(stgen['time'].iloc[-1])
    else:
        # File doesn't exist, perform another operation
        start_time = '2023-05-28 23:20:33.355+00:00'
        print('2023-05-28 23:20:33.355+00:00')
    print('Started')
    inPull(start_time)
    current_time = datetime.now()
    print('Pull completed at '+str(current_time))
    csv_file = pd.read_csv("ind.csv")  # Replace with influx db import system
    fetch_data("ind.csv")
    current_time = datetime.now()
    print('Fetch completed at ' + str(current_time))
    dataSet = pd.read_csv('new_inf_file.csv')
    dataSet['time'] = pd.to_datetime(dataSet['time'], format='ISO8601')
    sids = (dataSet['topic'].unique()).tolist()
    print('Scanning')
    for j in sids:
        if j not in sensors:
            sensors[j] = Sensor(j)
        new_data = dataSet[(dataSet['topic'] == j)]
        sensors[j].scan(new_data)
    print('Scan completed at ' + str(current_time))
    duration = datetime.now()-ini_time
    print('Process completed in ' + str(duration))
    sqlPush()


