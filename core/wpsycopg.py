import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import time
def query(settings):
    engine = create_engine(f'postgresql://{settings["PSQL_USERNAME"]}:{settings["PSQL_PASSWORD"]}@{settings["PSQL_HOSTNAME"]}:{settings["PSQL_PORT"]}/{settings["PSQL_DBNAME"]}')
    try:
        conn = psycopg2.connect(dbname=settings["PSQL_DBNAME"], user=settings["PSQL_USERNAME"], host=settings["PSQL_HOSTNAME"], password=settings["PSQL_PASSWORD"], port=settings["PSQL_PORT"])
    except:
        print("unable to connect to the database")
        exit(-1)
    cur = conn.cursor()
    DB_NAME = settings["DATABASE_NAME"]
    cur.execute(f"SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = \'public\' AND tablename = \'{DB_NAME}\')")
    table_exists = cur.fetchone()[0]
    if not(table_exists):
        data = pd.read_csv(settings['DATAPATH'] + settings['FILENAME'])
        data = data.drop(columns=data.columns[0], axis=1)
        data = data.rename(columns={'VendorID': 'cab_type'})
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        print('modification is completed')
        try:
            data.to_sql(settings['DATABASE_NAME'], engine, if_exists='fail',chunksize=10000, index=False)
            print("database created")
        except:
            print(f'database with name \'{settings["DATABASE_NAME"]}\' already exists or something went wrong')
    f = open("result.txt","a")
    f.seek(0,2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(f"""
                            SELECT cab_type, count(*) 
                            FROM {DB_NAME} GROUP BY 1;
                            """)
                end_time = time.time()
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(f"""
                            SELECT passenger_count, avg(total_amount) 
                            FROM {DB_NAME}
                            GROUP BY 1;
                            """)
                end_time = time.time()
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(f"""
                            SELECT
                            passenger_count, 
                            extract(year from tpep_pickup_datetime),
                            count(*)
                            FROM {DB_NAME}
                            GROUP BY 1, 2;
                            """)
                end_time = time.time()
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(f"""
                            SELECT
                            passenger_count,
                            extract(year from tpep_pickup_datetime),
                            round(trip_distance),
                            count(*)
                            FROM {DB_NAME}  
                            GROUP BY 1, 2, 3
                            ORDER BY 2, 4 desc;
                            """)
                end_time = time.time()
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        f.write(f"psycopg: query {query}, time - {sum} seconds\n")

