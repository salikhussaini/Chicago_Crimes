import os
import pandas as pd
import datetime

file_path = os.path.dirname(os.path.abspath(__file__))
new_dir = file_path+r'\Orginal'

for year in range(2001,2023):
    df = pd.read_csv(f'{new_dir}\Chicago_Crimes_{year}.csv')
    df = df.sort_values('Date')
    df['Month'] = df.Date.str[:11].str.split('/', expand = True)[0]
    for month in range(1,13):
        if month < 10:
            month_str = '0'+str(month)
            df_temp = df[df['Month'] == month_str].sort_values('Date')
            df_temp.to_csv(f'{file_path}\RAW\Crime_Year\{year}\{year}_{month_str}.csv', index = None)
        else:
            df_temp = df[df['Month'] == str(month)].sort_values('Date')
            df_temp.to_csv(f'{file_path}\RAW\Crime_Year\{year}\{year}_{month}.csv', index = None)