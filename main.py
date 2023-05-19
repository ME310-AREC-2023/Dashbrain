
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

alerts = {
    'type':[],
    'location' :[],
    'time' :[],
    'duration' :[],
    'severity': [],
    'active':[]
          }
alerts = pd.DataFrame(alerts)
alerts.to_csv('alerts.csv', index=False)
# Push new alerts to csv. Updates existing alerts if possible and replaces if necessary
def pushAlert(type, location, time, duration, severity, active):
    global alerts
    alerts = pd.read_csv('alerts.csv')
    alerts['time'] = pd.to_datetime(alerts['time'])
    conditions = (alerts['time'] == time) & (alerts['type'] == type) & (
                alerts['location'] == location)
    alert = alerts.loc[conditions]
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
        print(alert.index[0])
        alerts.loc[alert.index[0]] = atemp.loc[0]
        alerts.to_csv('alerts.csv', index=False)
    else:
        atemp.to_csv('alerts.csv', mode='a', index=False, header=False)
    alerts = pd.read_csv('alerts.csv')
    alerts['time'] = pd.to_datetime(alerts['time'])
    return(alerts)

def fetch_data(csv_file):
    # Read CSV data
    raw_data = pd.read_csv(csv_file)

    # Filter the data based on sensor_id and _field column
    new_data = raw_data[(raw_data['_field'] == 'co')]

    # Convert timestamp to epoch and multiply _value by 1000
    new_data['_time'] = pd.to_datetime(new_data['_time'])


    # Filter the data based on sensor_id and _field column for temperature
    new_temp_data = raw_data[(raw_data['_field'] == 'temperature')]

    # Convert timestamp to epoch
    new_temp_data['_time'] = pd.to_datetime(new_temp_data['_time'])


    new_humid_data = raw_data[(raw_data['_field'] == 'humidity')]
    new_humid_data['_time'] = pd.to_datetime(new_humid_data['_time'])

    # rename and append columns
    new_data.rename(columns={'_time': 'time'}, inplace=True)
    new_data.rename(columns={'_value': 'co'}, inplace=True)
    new_temp_data.rename(columns={'_value': 'temperature'}, inplace=True)
    new_humid_data.rename(columns={'_value': 'humidity'}, inplace=True)
    new_temp_data.reset_index(inplace=True)
    new_humid_data.reset_index(inplace=True)
    new_csv = pd.concat([new_data, new_temp_data['temperature'], new_humid_data['humidity']], axis=1)


    return new_csv


# Limit vectors for co temp humid [limit, sum limit]
clim = [.5, 1.2]
tlim = [72, 24]
hlim = [35.5, 12]
tcount = 1
sids = ['TLM0100', 'TLM0101', 'TLM0102', 'TLM0103', 'TLM0200', 'TLM0201', 'TLM0202', 'TLM0203']
# Iterate through sensors

while True:
    # Call function
    csv_file = "data.csv" # Replace with influx db import system
    new_csv = fetch_data(csv_file)
    new_csv.to_csv('new_file.csv', index=False)
    dataSet = pd.read_csv('new_file.csv')
    dataSet['time'] = pd.to_datetime(dataSet['time'])
    for j in sids:
        new_data = dataSet[(dataSet['sensor_id'] == j)]
        new_data.reset_index(inplace=True)
        count = [0, 0, 0, 0, 0, 0]
        tstamp = [pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00'), pd.Timedelta('0 days 00:00:00')]
        # Search every data entry
        for i in range(tcount, len(new_data)):

            # Search for co alerts
            if(new_data['co'][i] > clim[0]):
                if (count[1] == 0):
                    tstamp[0] = new_data['time'][i]

                count[0] += (new_data['co'][i] - clim[0])
                count[1] += int((new_data['time'][i] - new_data['time'][i - 1]).total_seconds())


            else:
                if (count[0] > clim[1]):
                    print('Co alert of severity ' + str(count[0]) + ' at sensor ' + str(j) + ' & time '
                          + str(tstamp[0]) + ' for ' + str(count[1]) + ' seconds')
                    pushAlert('co', j, tstamp[0], count[1], count[0], 1)
                count[0] = 0
                count[1] = 0
                tstamp[0] = pd.Timedelta('0 days 00:00:00')


            # Search for temperature alerts
            if (new_data['temperature'][i] > tlim[0]):
                if (count[3] == 0):
                    tstamp[1] = new_data['time'][i]
                count[2] += (new_data['temperature'][i] - tlim[0])
                count[3] += int((new_data['time'][i] - new_data['time'][i - 1]).total_seconds())

            else:
                if (count[2] > tlim[1]):
                    print('Temperature alert of severity ' + str(count[2]) + ' at sensor ' + str(j) + ' & time '
                          + str(tstamp[1]) + ' for ' + str(count[3]) + ' seconds')
                    pushAlert('temperature', j, tstamp[1], count[3], count[2], 1)
                count[2] = 0
                count[3] = 0


            # Search for humidity alerts
            if (new_data['humidity'][i] > hlim[0]):
                if (count[5] == 0):
                    tstamp[2] = new_data['time'][i]
                count[4] += (new_data['humidity'][i] - hlim[0])
                count[5] = int((new_data['time'][i] - new_data['time'][i - 1]).total_seconds())

            else:
                if (count[4] > hlim[1]):
                    print('Humidity alert of severity ' + str(count[4]) + ' at sensor ' + str(j) + ' & time '
                          + str(tstamp[2]) + ' for ' + str(count[5]) + ' seconds')
                    pushAlert('humidity', j, tstamp[2], count[5], count[4], 1)

                count[4] = 0
                count[5] = 0

        # Prints any generated alerts. Replace with necessary export function.
        if (count[0] > clim[1]):
            print('Co alert of severity ' + str(count[0]) + ' at sensor ' + str(j) + ' & time '
                  + str(tstamp[0]) + ' for ' + str(count[1]) + ' seconds')
            pushAlert('co', j, tstamp[0], count[1], count[0], 0)
        if (count[2] > tlim[1]):
            print('Temperature alert of severity ' + str(count[2]) + ' at sensor ' + str(j) + ' & time '
                  + str(tstamp[1]) + ' for ' + str(count[3]) + ' seconds')
            pushAlert('temperature', j, tstamp[1], count[3], count[2], 0)
        if (count[4] > hlim[1]):
            print('Humidity alert of severity ' + str(count[4]) + ' at sensor ' + str(j) + ' & time '
                  + str(tstamp[2]) + ' for ' + str(count[5]) + ' seconds')
            pushAlert('humidity', j, tstamp[2], count[5], count[4], 0)
        if j == 'TLM0203' and tcount != len(new_data):
            tcount = len(new_data)
            print(tcount)
            print(tstamp)


