import MySQLdb
import subprocess
import os
import re
import resources
from datetime import date, timedelta

stations = []
artist_query = """select stations.seed_artist_asins as station_id, stations.title as station_name,
                 categories.title as station_category from genre_stations stations, genre_categories categories
                 where stations.state = 1 and stations.station_type = 'ARTIST'
                 and stations.genre_category_id = categories.id
               """
genre_query = """select stations.genre_id as station_id, stations.title as station_name,
                 categories.title as station_category from genre_stations stations, genre_categories categories
                 where stations.station_type = 'GENRE' and stations.genre_category_id = categories.id
                 and stations.state=1
              """



def clean_station_id(station_id):

    regex = re.compile('([\\w-]+)')
    match = regex.search(station_id)
    station_id = match.group(1)
    return station_id


def query_db(db, query):

    cursor = db.cursor()
    cursor.execute(query)

    for station_id, station_name, station_category in cursor.fetchall():
        stations.append(clean_station_id(station_id.strip()) + '\t' + station_name.strip() + '\t'
                        + station_category.strip() + '\n')


def export_data():

    f = open('stations.tsv', 'w')
    for station in stations:
        f.write(station)
    f.close()




try:
    username, passwd = resources.get_credentials()
    database = resources.connect_to_db(username, passwd)
    query_db(database, artist_query)
    query_db(database, genre_query)
    os.chdir(resources.home_dir)
    export_data()
    resources.upload_data()
finally:
    database.close()
    

