import pandas as pd
alerts = pd.read_csv('alerts.csv')
alerts['time'] = pd.to_datetime(alerts['time'])
"""
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
"""


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

pushAlert(alerts['type'][0], alerts['location'][0], alerts['time'][0], alerts['duration'][0], 1000, alerts['active'][0])
alerts = pd.read_csv('alerts.csv')
alerts['time'] = pd.to_datetime(alerts['time'])



# df[(df['dt'] > '2014-07-23 07:30:00') & (df['dt'] < '2014-07-23 09:00:00')]

