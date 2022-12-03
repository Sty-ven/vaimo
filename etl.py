import requests

import pandas as pd

import gspread as gs

from google.oauth2 import service_account

from datetime import date, datetime, timedelta
# import pandas_gbq

gc = gs.service_account(filename='awesome-highway-358007-5eb23d1d599f.json')

sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1ljuw1THDhhpBYoPdjDvsHv2RDWKv6v0daIQoyBokvJE/edit#gid=0')

ws = sh.worksheet('location')

df = pd.DataFrame(ws.get_all_records())



# function to return all days between two dates
def datespan(startDate, endDate, delta=timedelta(days=1)):
    currentDate = startDate
    while currentDate < endDate:
        yield currentDate
        currentDate += delta
        
all_results = []  
def get_data():
    all_results = []
    
    #loop through all days between the two given dates
    for day in datespan(date(2007, 3, 30), date(2007, 4, 15),delta=timedelta(days=1)):
#         print(day)
        
        # loop through the individual longitude and latitude from the populated data sheet
        for name, lat, long in df[['Name', 'Latitude', 'Longitude']].values:
            url = f"https://api.sunrisesunset.io/json?lat={lat}&lng={long}&timezone=UTC&date={day}"
            try:
                r = requests.get(url)
                result_dict = r.json()['results']
                result_dict['date'] = day
                result_dict['name'] = name
                all_results.append(result_dict)
            except Exception as e:
                print(f"Day: {day}... Error: {e}")
                pass
    data = pd.json_normalize(all_results)
    data = data[['name', 'date', 'dawn', 'dusk', 'sunrise', 'sunset']]
    # for dawn, dusk in data.loc[data.name == "Honolulu, Hawaii", ["dawn", "dusk"]].values:
    #     data.dawn = dusk
    #     data.dusk = dawn
    
    return data

data = get_data()

table_schema = [{'name': 'location_name', 'type': 'STRING'}, 
               {'name': 'date', 'type': 'DATE'},
               {'name': 'time_of_dawn', 'type': 'TIMESTAMP'},
               {'name': 'time_of_dusk', 'type': 'TIMESTAMP'},
               {'name': 'time_of_sunrise', 'type': 'TIMESTAMP'},
               {'name': 'time_of_sunset', 'type': 'TIMESTAMP'},
               ]

credentials = service_account.Credentials.from_service_account_file(
    'awesome-highway-358007-5eb23d1d599f.json',
)

data.to_gbq("Daily_plan.Vaimo", "awesome-highway-358007", 
            credentials=credentials, table_schema=table_schema, if_exists='replace')


