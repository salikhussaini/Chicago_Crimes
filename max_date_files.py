import os
import pandas as pd
import datetime
import sys

import pandas as pd
from sodapy import Socrata

file_path = os.path.dirname(os.path.abspath(__file__))
new_dir = file_path+r'\RAW\Crime_Year\2022'

max_date = datetime.datetime(2022,1,1)
for subdir, dirs, files in os.walk(new_dir):
    for file in files:
        #print os.path.join(subdir, file)
        filepath_ = subdir + os.sep + file
        df_temp = pd.read_csv(filepath_)

        df_temp = df_temp.sort_values('Date')
        df_temp.date__ = pd.to_datetime(df_temp.Date.str.slice(start=0,stop=10))
        try:
            if df_temp.date__.max() > max_date:
                max_date = df_temp.date__.max()
        except:
            print(df_temp.date__.max())
client = Socrata("data.cityofchicago.org", None)

max_datetime_year = int(max_date.strftime("%Y"))
max_datetime_month = int(max_date.strftime("%m"))
max_datetime_day = int(max_date.strftime("%d"))

results = client.get_all("ijzp-q8t2",\
     where=\
        f"year = {max_datetime_year} \
        AND date_extract_m(date) >= {max_datetime_month}\
        AND date_extract_d(date) > {max_datetime_day}")
results_df = pd.DataFrame.from_records(results)
if results_df.shape[0] == 0:
    sys.exit("Data is Up to Date")

results_df = results_df.drop('location', axis = 1)
results_df['Location'] = '(' + \
                        results_df['latitude'].astype(str) + \
                        ', ' + \
                        results_df['longitude'].astype(str) +\
                        ')'
cols = ['ID', 'Case Number', 'Date', 'Block', 'IUCR', 'Primary Type',
       'Description', 'Location Description', 'Arrest', 'Domestic', 'Beat',
       'District', 'Ward', 'Community Area', 'FBI Code', 'X Coordinate',
       'Y Coordinate', 'Year', 'Updated On', 'Latitude', 'Longitude',
       'Location', 'Month']
results_df['Month'] = pd.DatetimeIndex(pd.to_datetime(results_df.date.str[:10])).month
results_df.columns = cols
#results_df.to_csv('Test2.csv', index = None)

months = results_df.Month.unique()
years = results_df.Year.unique()
for year in years:
    for month in months:
        df_10 = pd.read_csv(f'RAW/Crime_Year/{year}/{year}_{month}.csv')
        df_10_2 =  results_df[results_df['Month'] == month]
        df_10.ID = df_10.ID.astype(int)
        df_10_2.ID = df_10_2.ID.astype(int)
        df_new = pd.merge(df_10_2,df_10,on = 'ID', how="outer", indicator=True
              ).query('_merge=="left_only"')
        df_new = df_new.drop('_merge', axis = 1)
        df_new = df_new[['ID', 'Case Number_x', 'Date_x', 'Block_x', 'IUCR_x', 'Primary Type_x',
            'Description_x', 'Location Description_x', 'Arrest_x', 'Domestic_x',
            'Beat_x', 'District_x', 'Ward_x', 'Community Area_x', 'FBI Code_x',
            'X Coordinate_x', 'Y Coordinate_x', 'Year_x', 'Updated On_x',
            'Latitude_x', 'Longitude_x', 'Location_x', 'Month_x']]
        df_new.columns = cols
        df_new.Date = pd.to_datetime(df_new.Date).dt.strftime("%m/%d/%Y %H:%M")

        df_new = df_new.sort_values('Date')
        df_10 = pd.concat([df_10,df_new], axis  = 0)
        df_10.to_csv(f'RAW/Crime_Year/{year}/{year}_{month}.csv', index=None)

#url = f"https://data.cityofchicago.org/resource/ijzp-q8t2.json$where=date between '{max_datetime_str}' and '{todate_datetime_str}'"
#print(requests.get(url))