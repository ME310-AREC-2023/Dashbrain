from influxdb_client import InfluxDBClient
from datetime import datetime
from dateutil import tz
import pandas as pd
import os

pd.options.mode.chained_assignment = None  # default='warn'
from sqlalchemy import create_engine
# dependent on pymysql

# Named to avoid errors
noerrorooo = {
    'type': [],
    'location': [],
    'time': [],
    'duration': [],
    'severity': [],
    'active': []
}
noerrorooo = pd.DataFrame(noerrorooo)
noerrorooo.to_csv('alerts.csv', index=False)
# Push new alerts to csv. Updates existing alerts if possible and replaces if necessary

iniindexes = {
    'location': [],
    'time': [],
    'value': [],
}
iniindexes = pd.DataFrame(iniindexes)
iniindexes.to_csv('indexes.csv', index=False)


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
    if os.path.exists('new_inf_file.csv'):
        print('appending data')
        df.to_csv('ind.csv', mode='a', index=False, header=False)
    else:
        # File doesn't exist, perform another operation
        df.to_csv('ind.csv', index=False)



def fetch_data(csv_file):
    # Read CSV data
    raw_data = pd.read_csv(csv_file)

    # Filter the data based on sensor_id and _field column
    new_data = raw_data[(raw_data['_field'] == 'eCO2')]

    # Convert timestamp to epoch and multiply _value by 1000
    new_data['_time'] = pd.to_datetime(new_data['_time'])
    new_data.rename(columns={'_time': 'time'}, inplace=True)
    new_data.rename(columns={'_value': 'eCO2'}, inplace=True)
    new_data.reset_index(inplace=True)

    # Filter the data based on sensor_id and _field column for temperature
    new_temp_data = raw_data[(raw_data['_field'] == 'temp')]

    # Convert timestamp to epoch
    new_temp_data['_time'] = pd.to_datetime(new_temp_data['_time'])
    new_temp_data.rename(columns={'_value': 'temp'}, inplace=True)
    new_temp_data.reset_index(inplace=True)

    new_humid_data = raw_data[(raw_data['_field'] == 'humidity')]
    new_humid_data['_time'] = pd.to_datetime(new_humid_data['_time'])
    new_humid_data.rename(columns={'_value': 'humidity'}, inplace=True)
    new_humid_data.reset_index(inplace=True)

    new_iaq_data = raw_data[(raw_data['_field'] == 'iaq')]
    new_iaq_data['_time'] = pd.to_datetime(new_iaq_data['_time'])
    new_iaq_data.rename(columns={'_value': 'iaq'}, inplace=True)
    new_iaq_data.reset_index(inplace=True)

    new_bvoc_data = raw_data[(raw_data['_field'] == 'bVOC')]
    new_bvoc_data['_time'] = pd.to_datetime(new_bvoc_data['_time'])
    new_bvoc_data.rename(columns={'_value': 'bVOC'}, inplace=True)
    new_bvoc_data.reset_index(inplace=True)

    new_pre_data = raw_data[(raw_data['_field'] == 'pressure')]
    new_pre_data['_time'] = pd.to_datetime(new_pre_data['_time'])
    new_pre_data.rename(columns={'_value': 'pressure'}, inplace=True)
    new_pre_data.reset_index(inplace=True)

    # rename and append columns

    new_csv = pd.concat(
        [new_data, new_temp_data['temp'], new_humid_data['humidity'], new_iaq_data['iaq'], new_bvoc_data['bVOC'],
         new_pre_data['pressure']], axis=1)

    return new_csv


class Sensor:
    def __init__(self, location):
        self.new_data = None
        self.count = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.tstamp = [pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'),
                       pd.Timedelta('0 days 00:00:00')]
        self.tcount = 0
        self.location = location
        self.clim = [1000, 800]
        self.tlim = [30, 72]
        self.hlim = [45, 40]
        self.blim = [2.2, 2]
        self.alim = [110, 100]
        self.ilim = [45, 15]
        self.tdelta = 3
        self.index = 0
        # tracks whether index alert is active
        self.aindex = 0
        self.indexes = pd.read_csv('indexes.csv')
        self.indexes['time'] = pd.to_datetime(self.indexes['time'])
        self.alerts = {
            'type': [],
            'location': [],
            'time': [],
            'duration': [],
            'severity': [],
            'active': []
        }
        print(self.location)

    def setData(self, data):
        self.new_data = data

    def pushAlert(self, type, location, time, duration, severity, active):
        print(type + ' alert of severity ' + str(severity) + ' at sensor ' + location + ' & time '
              + str(time) + ' for ' + str(duration) + ' seconds')
        self.alerts = pd.read_csv('alerts.csv')
        self.alerts['time'] = pd.to_datetime(self.alerts['time'])
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
        self.alerts['time'] = pd.to_datetime(self.alerts['time'])

    def healthIndex(self, timestamp):
        csum = 0
        tsum = 0
        hsum = 0
        bsum = 0
        asum = 0
        if self.count[1] != 0:
            csum = (self.count[0] / self.count[1]) / self.clim[0]
        if self.count[3] != 0:
            tsum = (self.count[2] / self.count[3]) / self.tlim[0]
        if self.count[5] != 0:
            hsum = (self.count[4] / self.count[5]) / self.hlim[0]
        if self.count[7] != 0:
            bsum = (self.count[6] / self.count[7]) / self.blim[0]
        if self.count[9] != 0:
            asum = (self.count[8] / self.count[9]) / self.alim[0]

        self.index = csum + tsum + hsum + bsum + asum
        self.index = self.index / .005
        itemp = {
            'location': [self.location],
            'time': [timestamp],
            'value': [self.index],
        }
        itemp = pd.DataFrame(itemp)
        self.indexes = pd.concat([self.indexes, itemp], ignore_index=True)

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
                if (self.count[0] > self.clim[1]):
                    self.pushAlert('eCO2', j, self.tstamp[0], self.count[1], self.count[0], 0)
                self.count[0] = 0
                self.count[1] = 0

            # Search for temperature alerts
            if (self.new_data['temp'][i] > self.tlim[0]):
                if (self.count[2] == 0):
                    self.tstamp[1] = self.new_data['time'][i]
                self.count[2] += (self.new_data['temp'][i] - self.tlim[0])
                self.count[3] = int((self.new_data['time'][i] - self.tstamp[1]).total_seconds())

            else:
                if (self.count[2] > self.tlim[1]):
                    self.pushAlert('temp', j, self.tstamp[1], self.count[3], self.count[2], 0)
                self.count[2] = 0
                self.count[3] = 0

            # Search for humidity alerts
            if (self.new_data['humidity'][i] > self.hlim[0]):
                if (self.count[4] == 0):
                    self.tstamp[2] = self.new_data['time'][i]
                self.count[4] += (self.new_data['humidity'][i] - self.hlim[0])
                self.count[5] = int((self.new_data['time'][i] - self.tstamp[2]).total_seconds())

            else:
                if (self.count[4] > self.hlim[1]):
                    self.pushAlert('humidity', j, self.tstamp[2], self.count[5], self.count[4], 0)

                self.count[4] = 0
                self.count[5] = 0

            # Search for bVOC alerts
            if (self.new_data['bVOC'][i] > self.blim[0]):
                if (self.count[6] == 0):
                    self.tstamp[3] = self.new_data['time'][i]
                self.count[6] += (self.new_data['bVOC'][i] - self.blim[0])
                self.count[7] = int((self.new_data['time'][i] - self.tstamp[3]).total_seconds())


            else:
                if (self.count[6] > self.blim[1]):
                    self.pushAlert('bVOC', j, self.tstamp[3], self.count[7], self.count[6], 0)

                self.count[6] = 0
                self.count[7] = 0

            # Search for iaq alerts
            if (self.new_data['iaq'][i] > self.alim[0]):
                if (self.count[8] == 0):
                    self.tstamp[4] = self.new_data['time'][i]
                self.count[8] += (self.new_data['iaq'][i] - self.alim[0])
                self.count[9] = int((self.new_data['time'][i] - self.tstamp[4]).total_seconds())

            else:
                if (self.count[8] > self.alim[1]):
                    self.pushAlert('iaq', j, self.tstamp[4], self.count[9], self.count[8], 0)

                self.count[8] = 0
                self.count[9] = 0

            if ((i % 10) == 0):
                self.healthIndex(self.new_data['time'][i])
                if (self.index > self.ilim[0]):
                    if (self.count[10] == 0):
                        self.tstamp[5] = self.new_data['time'][i]
                        self.aindex = 1
                    self.count[10] += (self.index - self.ilim[0])
                    self.count[11] = int((self.new_data['time'][i] - self.tstamp[5]).total_seconds())
                else:
                    if (self.aindex == 1):
                        self.pushAlert('index', j, self.tstamp[5], self.count[11], self.count[10], 0)
                    self.count[10] = 0
                    self.count[11] = 0
                    self.aindex = 0

        if (self.tcount != len(self.new_data)):
            if (self.count[0] > self.clim[1]):
                self.pushAlert('eCO2', j, self.tstamp[0], self.count[1], self.count[0], 0)
            if (self.count[2] > self.tlim[1]):
                self.pushAlert('temp', j, self.tstamp[1], self.count[3], self.count[2], 0)
            if (self.count[4] > self.hlim[1]):
                self.pushAlert('humidity', j, self.tstamp[2], self.count[5], self.count[4], 0)
            if (self.count[6] > self.blim[1]):
                self.pushAlert('bVOC', j, self.tstamp[3], self.count[7], self.count[6], 0)
            if (self.count[8] > self.alim[1]):
                self.pushAlert('iaq', j, self.tstamp[4], self.count[9], self.count[8], 0)
            if (self.aindex == 1):
                self.pushAlert('index', j, self.tstamp[5], self.count[11], self.count[10], 1)
            self.tcount = len(self.new_data)
        self.indexes.to_csv('indexes.csv', mode='a', index=False, header=False)


def sqlPush():
    engine = create_engine('mysql+pymysql://alertGenerator:ME310arec@192.168.10.103:3306/alert-test')

    alerts = pd.read_csv("alerts.csv")
    alerts['time'] = pd.to_datetime(alerts['time'])
    # alerts['duration'] = pd.to_timedelta(alerts['duration'])
    alerts['time'] = pd.to_datetime(alerts['time'])

    alerts.to_sql('AlertList', con=engine, if_exists='replace', index=False)


# Limit vectors for co temp humid [limit, sum limit]

sensors = {}

# Iterate through sensors

# Call function
while True:
    if os.path.exists('new_inf_file.csv'):
        stgen = pd.read_csv('new_inf_file.csv')
        # Perform your operations here
        start_time = stgen['time'].iloc[-1]
        print(stgen['time'].iloc[-1])
    else:
        # File doesn't exist, perform another operation
        start_time = '2023-04-20 23:20:33.355+00:00'
        print('2023-04-20 23:20:33.355+00:00')
    print('started')
    inPull(start_time)
    current_time = datetime.now()
    print('pull completed at '+str(current_time))
    csv_file = pd.read_csv("ind.csv")  # Replace with influx db import system
    new_csv = fetch_data("ind.csv")
    new_csv = new_csv.reset_index(drop=True)
    new_csv.to_csv('new_inf_file.csv', index=False)

    dataSet = pd.read_csv('new_inf_file.csv')
    dataSet['time'] = pd.to_datetime(dataSet['time'])
    sids = (dataSet['topic'].unique()).tolist()
    print('scanning')
    for j in sids:
        if j not in sensors:
            sensors[j] = Sensor(j)
        new_data = dataSet[(dataSet['topic'] == j)]
        sensors[j].scan(new_data)
    sqlPush()


