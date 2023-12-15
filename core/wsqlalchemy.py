from sqlalchemy import create_engine,orm, text
import time
def query(settings):
    engine = create_engine('postgresql://postgres:12345@localhost:5432/postgres')
    cur = orm.sessionmaker(bind=engine)()
    f = open("result.txt", "a")
    f.seek(0, 2)
    for query in settings["QUERIES"]:
        sum = 0
        if (query == str(1)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(text("""
                                SELECT cab_type, count(*) 
                                FROM trips GROUP BY 1;
                                """))
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(2)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(text("""
                                SELECT passenger_count, avg(total_amount) 
                                FROM trips
                                GROUP BY 1;
                                """))
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(3)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(text("""
                                SELECT
                                passenger_count, 
                                extract(year from tpep_pickup_datetime),
                                count(*)
                                FROM trips
                                GROUP BY 1, 2;
                                """))
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        elif (query == str(4)):
            for i in range(settings["NUM_OF_TESTS"]):
                start_time = time.time()
                cur.execute(text("""
                                SELECT
                                passenger_count,
                                extract(year from tpep_pickup_datetime),
                                round(trip_distance),
                                count(*)
                                FROM trips  
                                GROUP BY 1, 2, 3
                                ORDER BY 2, 4 desc;
                                """))
                end_time = time.time()
                sum = sum + (end_time - start_time)
            sum = sum / settings["NUM_OF_TESTS"]
        f.write(f"SQLAlchemy: query {query}, time - {sum} seconds\n")