import pandas as pd
import time
from os.path import exists
def query(settings):
    pd.options.mode.chained_assignment = None
    file_exists = exists(settings["DATABASE_NAME"]+".pkl")
    if (not(file_exists)):
        data = pd.read_csv(settings["DATAPATH"] + settings["FILENAME"])
        data = data.drop(columns=data.columns[0], axis=1)
        data = data.rename(columns={'VendorID': 'cab_type'})
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        print('modification is completed')
        try:
            data.to_pickle(settings['DATABASE_NAME']+".pkl")
            print("database created")
        except:
            print(f'database with name \'{settings["DATABASE_NAME"]}\' already exists or something went wrong')
    data = pd.read_pickle(settings['DATABASE_NAME'] + ".pkl")
    f = open("result.txt", "a")
    f.seek(0, 2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                selected_df = data[['cab_type']]
                grouped_df = selected_df.groupby('cab_type')
                final_df = grouped_df.size().reset_index(name='counts')
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                selected_df = data[['passenger_count', 'total_amount']]
                grouped_df = selected_df.groupby('passenger_count')
                final_df = grouped_df.mean().reset_index()
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                selected_df = data[['passenger_count', 'tpep_pickup_datetime']]
                selected_df['year'] = pd.to_datetime(
                    selected_df.pop('tpep_pickup_datetime'),
                    format='%Y-%m-%d %H:%M:%S').dt.year
                grouped_df = selected_df.groupby(['passenger_count', 'year'])
                final_df = grouped_df.size().reset_index(name='counts')
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                selected_df = data[[
                    'passenger_count',
                    'tpep_pickup_datetime',
                    'trip_distance']]
                selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
                selected_df['year'] = pd.to_datetime(
                    selected_df.pop('tpep_pickup_datetime'),
                    format='%Y-%m-%d %H:%M:%S').dt.year
                grouped_df = selected_df.groupby([
                    'passenger_count',
                    'year',
                    'trip_distance'])
                final_df = grouped_df.size().reset_index(name='counts')
                final_df = final_df.sort_values(
                    ['year', 'counts'],
                    ascending=[True, False])
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        f.write(f"Pandas: query {query}, time - {sum} seconds\n")
