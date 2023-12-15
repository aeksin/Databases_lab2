import duckdb
import time
def query(settings):
    con = duckdb.connect("database.db")
    DB_NAME = settings["DATABASE_NAME"]
    con.sql(f"CREATE TABLE IF NOT EXISTS {DB_NAME} AS SELECT * FROM read_csv('{settings['DATAPATH'] + settings['FILENAME']}',AUTO_DETECT=TRUE);")
    con.execute(f"select * from information_schema.columns where column_name = 'cab_type' and table_name = \'{DB_NAME}\'")
    if (len(con.fetchall())==0):
        con.sql(f"ALTER TABLE {DB_NAME} RENAME COLUMN VendorID TO cab_type;")
    f = open("result.txt", "a")
    f.seek(0,2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute(f"""
                            SELECT cab_type, count(*)
                            FROM {DB_NAME} GROUP BY 1;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute(f"""
                            SELECT passenger_count, avg(total_amount) 
                            FROM {DB_NAME}
                            GROUP BY 1;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute(f"""
                            SELECT
                            passenger_count, 
                            extract(year from tpep_pickup_datetime),
                            count(*)
                            FROM {DB_NAME}
                            GROUP BY 1, 2;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute(f"""
                            SELECT
                            passenger_count,
                            extract(year from tpep_pickup_datetime),
                            round(trip_distance),
                            count(*)
                            FROM {DB_NAME} 
                            GROUP BY 1, 2, 3
                            ORDER BY 2, 4 desc;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        f.write(f"DuckDB: query {query}, time - {sum} seconds\n")
    con.close()