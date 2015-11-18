import shutil
import zipfile
import requests
import csv
import os
import psycopg2
from pprint import pprint


def unzip(source_filename, dest_dir):
    with zipfile.ZipFile(source_filename) as zf:
        zf.extractall(dest_dir)

#TODO safe parameters
def droptable(name):
    with conn:
        with conn.cursor() as curs:
            try:
                curs.execute('DROP TABLE ' + name)
            except:
                pass

fetchdata = False
spcBaseUrl = 'http://www.spc.noaa.gov/wcm/data/{year}-2014_{wxType}.csv.zip'

if fetchdata:
    shutil.rmtree('data', True)
    os.makedirs('data')
    wxTypes = ('torn', 'hail', 'wind')
    for wxType in wxTypes:
        url = spcBaseUrl.replace('{year}', '1950' if wxType == 'torn' else '1955')
        url = url.replace('{wxType}', wxType)
        pprint('downloading ' + url)
        response = requests.get(url, stream=True)
        filepath = 'data/' + wxType + '.zip'
        with open(filepath, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        unzip(filepath, 'data')

conn = psycopg2.connect(dbname='stormdata', user='postgres', password='fails345')
droptable('tornado')
droptable('hail')
droptable('wind')
#TODO multiple statements without the with crap
#TODO switch all the numerics to reals for better performance
#TODO verify types match data
with conn:
    with conn.cursor() as curs:
        curs.execute('''CREATE TABLE tornado (om integer, yr smallint, mo smallint, day smallint, date date,
            time time, tz smallint, st char(2), stf smallint, stn smallint, f smallint, inj smallint, fat smallint,
            loss smallint, closs smallint, slat numeric(4, 2), slon numeric(5, 2), elat numeric(4, 2),
            elon numeric(5, 2), len numeric(6, 2), wid smallint, ns smallint, sn smallint, sg smallint, f1 smallint,
            f2 smallint, f3 smallint, f4 smallint);''')
#TODO finish indexes
with conn:
    with conn.cursor() as curs:
        curs.execute('CREATE INDEX f_idx ON tornado (f);')
with conn:
    with conn.cursor() as curs:
        curs.execute('''CREATE TABLE hail (om integer, yr smallint, mo smallint, day smallint, date date,
            time time, tz smallint, st char(2), stf smallint, stn smallint, sz numeric(4, 2), inj smallint,
            fat smallint, loss smallint, closs smallint, slat numeric(4, 2), slon numeric(5, 2), elat numeric(4, 2),
            elon numeric(5, 2), len numeric(6, 2), wid smallint, ns smallint, sn smallint, sg smallint, f1 smallint,
            f2 smallint, f3 smallint, f4 smallint);''')
with conn:
    with conn.cursor() as curs:
        curs.execute('''CREATE TABLE wind (om integer, yr smallint, mo smallint, day smallint, date date,
            time time, tz smallint, st char(2), stf smallint, stn smallint, mag smallint, inj smallint, fat smallint,
            loss smallint, closs smallint, slat numeric(4, 2), slon numeric(5, 2), elat numeric(4, 2),
            elon numeric(5, 2), len numeric(6, 2), wid smallint, ns smallint, sn smallint, sg smallint, f1 smallint,
            f2 smallint, f3 smallint, f4 smallint, mt char(2));''')

reader = csv.reader(open('data/1950-2014_torn.csv', 'r'), delimiter=',')
for line in reader:
    line[4] = '\'' + line[4] + '\''
    line[5] = '\'' + line[5] + '\''
    line[7] = '\'' + line[7] + '\''
    values = ','.join(line)
    with conn:
        with conn.cursor() as curs:
            curs.execute('INSERT INTO tornado VALUES (' + values + ')')

conn.close()


