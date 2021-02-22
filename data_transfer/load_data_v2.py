
import pandas as pd
import csv
import os
from sqlalchemy import create_engine
import datetime as dt
import time
import schedule
import numpy as np
import serial


## Store CSV, then import dataframe to DB

# def convert_time(row):
#     row = dt.datetime.strptime(row,"%d.%m.%Y %H:%M:%S")
#     return row

# def read_file(path,now):
#     filelist = os.listdir(path)
#     filename = []
#     for file in filelist:
#         if file.endswith(".csv"):
#             #filename.append(file.split('.')[0])
#             filename.append(file)
#     with open('./all_file.txt','w') as f:
#         f.write(f"**********{now}***********\n")
#         for item in filename:
#             f.write(f"{item}\n")
#     return filename

# #check new file every time
# def new_file_check(path,now):
#     with open('./all_file.txt','r') as f:
#         old_filename = f.readlines()
#         old_filename = [x.strip() for x in old_filename if not x.startswith('*')]
#     latest_filelist = os.listdir(path)
#     latest_filename = []

#     for file in latest_filelist:
#         if file.endswith(".csv"):
#             #filename.append(file.split('.')[0])
#             latest_filename.append(file)

#     if len(latest_filename) == len(old_filename):

#         print('All the files are up-to-date!!  ', now)
#         print('--------------------------------------------------')

#         return 0

#     else:
#         print('Find new files!!  ', now)
#         print('--------------------------------------------------')
#         #now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
#         new_filename = [y for y in latest_filename if y not in old_filename]
#         with open('./all_file.txt','a') as f:
#             f.write(f"**********{now}***********\n")
#             for item in new_filename:
#                 f.write(f"{item}\n")


#         return new_filename

# def sql(path, filename,table_name,uri,now):

#     df = pd.read_csv(path+filename, low_memory=False)
#     df['date_time'] = df['date_time'].apply(convert_time)
#     #del df['Unnamed: 0']
#     #ts = []
#     #for i in range(len(df['temp'])):
#         #ts.append(now)
#     #df.insert(loc=0,column='time',value=ts)
#     df.to_sql(table_name, con=uri, schema = 'local_data', if_exists='append', index=False)

# def main():
#     path = "C:/Users/chi.zhang1/Desktop/arduino_2_csv/csv_data/"
#     uri = 'mysql+pymysql://root:123456@localhost/local_data'
#     table_name = "sensor_data"
#     #mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
#     engine = create_engine(uri)
#     now = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
#     if os.path.exists('./all_file.txt') == False:
#         #first time, check if .txt file exists
#         print('first-time import!!  ', now)
#         print('--------------------------------------------------')
#         engine.execute(f"DROP TABLE IF EXISTS {table_name}")
#         engine.execute(f"""
#             CREATE TABLE {table_name}
#             (
#                 Loc_ID INT
#                 Time_EST VARCHAR(255),
#                 Yr INT,
#                 Sensor_reading DOUBLE,
#                 M_ID INT
#             );
#         """ )

#         filename = read_file(path,now)

#         for file in filename:
#             sql(path,file,table_name,uri,now)
#     else:

#         new_filename = new_file_check(path,now)
#         try:
#             for file in new_filename:
#                 sql(path,file,table_name,uri,now)
#         except:
#             pass

# if __name__ == '__main__':
#     schedule.every().day.at("08:00").do(main)
#     #schedule.every(0.5).minutes.do(main)
#     while True:
#         # Checks whether a scheduled task
#         # is pending to run or not
#         schedule.run_pending()
#         time.sleep(1)


## Import to DB per data stream


# uri = 'mysql+pymysql://root:362324wsz@localhost/localdata'
# engine = create_engine(uri)
# measure = pd.read_sql('measurement',uri,columns=['M_ID','Measurement'])


# a = b'28.01.2021 16:12:00;temp_air_2m,25;hum_air_2m,70;CO2_air_2m,772\n'
# b = b'28.01.2021 16:39:53;temp_air_2m,21.00;humidity_air_2m,31.00;CO2_air_2m,915.63\n'
# a = a.decode('utf-8')
# b = b.decode('utf-8')
# c = [a,b]


# a = a.strip().split(';')
# datetime = convert_time(a.pop(0))
# datetime_Yr = [datetime, datetime.year]
# a_split = [(datetime_Yr+i.split(',')) for i in a]
# arr = np.array(a_split).T
# dict1 = {'Time_EST':arr[0],'Yr':arr[1],'Measurement':arr[2],'Sensor_reading':arr[3]}
# df = pd.DataFrame(dict1)
# df = pd.merge(df,measure,on='Measurement',how='left').sort_values('M_ID',ascending = True)
# df = df.drop('Measurement',axis=1)
# df


def convert_time(row):
    row = dt.datetime.strptime(row, "%d.%m.%Y %H:%M:%S")
    return row

def sql(stream, measure, table_name, uri):
    stream = stream.strip().split(';')
    datetime = convert_time(stream.pop(0))
    datetime_Yr = [datetime, datetime.year]
    stream_split = [(datetime_Yr + i.split(',')) for i in stream]
    arr = np.array(stream_split).T
    dict1 = {'Time_EST': arr[0], 'Yr': arr[1], 'Measurement': arr[2], 'Sensor_reading': arr[3]}
    df = pd.DataFrame(dict1)
    df = pd.merge(df, measure, on='Measurement', how='left').sort_values('M_ID', ascending=True)
    df = df.drop('Measurement', axis=1)
    df.to_sql(table_name, con=uri, schema='local_data', if_exists='append', index=False)
    return df


def main():
    serialPort = "COM3"  # find your com port #
    baudRate = 9600
    ser = serial.Serial(serialPort, baudRate, timeout=0.5)
    path = './data/'
    uri = 'mysql+pymysql://root:571420670z@localhost/local_data'
    engine = create_engine(uri)
    measure = pd.read_sql('measurement', uri, columns=['M_ID', 'Measurement'])
    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    table_name = 'sensor_data'
    appended_df = []
    iterator = 0
    n = 1
    save = 6
    while True:

        stream = ser.readline()
        stream = stream.decode('utf-8')

        if len(stream) == 0 or iterator < 3:
            pass
        else:


            # if n == 1:
            #     engine.execute(f"DROP TABLE IF EXISTS {table_name}")
            #     engine.execute(f"""
            #         CREATE TABLE {table_name}
            #         (
            #
            #             Time_EST VARCHAR(255),
            #             Yr INT,
            #             Sensor_reading DOUBLE,
            #             M_ID INT
            #         );
            #     """)
            df = sql(stream, measure, table_name, uri)
            appended_df.append(df)
            if n % 20 == 0:

                appended_df = pd.concat(appended_df)
                with open('./data/import_history.txt', 'a+') as f:
                    f.write(f"--------------{now}--------------\n")
                    f.write(f'data{save + 1}.csv\n')
                appended_df.to_csv(path + f'data{save + 1}.csv', index=False)
                appended_df = []
                save = save + 1
            else:
                pass
            n = n+1

        iterator = iterator + 1

if __name__ == '__main__':
    main()