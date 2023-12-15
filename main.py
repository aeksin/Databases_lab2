from core import initialize, wpsycopg, wduckdb, wsqlite, wpandas, wsqlalchemy
if (__name__ == '__main__'):
    settings = initialize.get_settings()
    if 'psycopg2' in settings["LIBRARIES"]:
        wpsycopg.query(settings)
    if 'DuckDB' in settings["LIBRARIES"]:
        wduckdb.query(settings)
    if 'SQLite' in settings["LIBRARIES"]:
        wsqlite.query(settings)
    if 'Pandas' in settings["LIBRARIES"]:
        wpandas.query(settings)
    if 'SQLAlchemy' in settings["LIBRARIES"]:
        wsqlalchemy.query(settings)