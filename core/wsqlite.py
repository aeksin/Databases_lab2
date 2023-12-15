import sqlite3
import time
import pandas as pd
def query(settings):
    conn = sqlite3.connect('sqlite.db')
    cur = conn.cursor()
    cur.execute(f"SELECT name FROM sqlite_schema WHERE type='table' AND name=\"{settings['DATABASE_NAME']}\";")
    table_exists = cur.fetchall()
    if not(len(table_exists)):
        data = pd.read_csv(settings["DATAPATH"] + settings["FILENAME"])
        data = data.drop(columns=data.columns[0], axis=1)
        data = data.rename(columns={'VendorID': 'cab_type'})
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        print('modification is completed')
        try:
            data.to_sql(settings['DATABASE_NAME'],if_exists='fail',chunksize=10000,con = conn)
        except:
            print(f'database with name \'{settings["DATABASE_NAME"]}\' already exists or something went wrong')

    f = open("result.txt", "a")
    f.seek(0, 2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute("""
                                SELECT cab_type, count(*) 
                                FROM trips GROUP BY 1;
                                """)
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute("""
                                SELECT passenger_count, avg(total_amount) 
                                FROM trips
                                GROUP BY 1;
                                """)
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute("""
                                SELECT
                                passenger_count, 
                                STRFTIME('%Y', "tpep_pickup_datetime"),
                                count(*)
                                FROM trips
                                GROUP BY 1, 2;
                                """)
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute("""
                                SELECT
                                passenger_count,
                                STRFTIME('%Y', "tpep_pickup_datetime"),
                                round(trip_distance),
                                count(*)
                                FROM trips  
                                GROUP BY 1, 2, 3
                                ORDER BY 2, 4 desc;
                                """)
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        f.write(f"SQLite: query {query}, time - {sum} seconds\n")



    conn.close()