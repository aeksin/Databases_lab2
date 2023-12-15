import duckdb
import time
def query(settings):
    con = duckdb.connect("database.db")
    con.sql(f"CREATE TABLE IF NOT EXISTS trips AS SELECT * FROM read_csv('{settings['DATAPATH'] + settings['FILENAME']}',AUTO_DETECT=TRUE);")
    f = open("result.txt", "a")
    f.seek(0,2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute("""
                            SELECT cab_type, count(*) 
                            FROM trips GROUP BY 1;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute("""
                            SELECT passenger_count, avg(total_amount) 
                            FROM trips
                            GROUP BY 1;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute("""
                            SELECT
                            passenger_count, 
                            extract(year from tpep_pickup_datetime),
                            count(*)
                            FROM trips
                            GROUP BY 1, 2;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time_ns() / (10 ** 9)
                con.execute("""
                            SELECT
                            passenger_count,
                            extract(year from tpep_pickup_datetime),
                            round(trip_distance),
                            count(*)
                            FROM trips  
                            GROUP BY 1, 2, 3
                            ORDER BY 2, 4 desc;
                            """)
                end_time = time.time_ns() / (10 ** 9)
                sum= sum+(end_time-start_time)
            sum=sum/settings["NUM_OF_TESTS"]
        f.write(f"DuckDB: query {query}, time - {sum} seconds\n")
    con.close()