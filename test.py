import fiona
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from geoalchemy2 import Geometry, WKTElement
import glob
from zipfile import ZipFile
import os
import shutil

# engine = create_engine('postgresql://bedi:tiger@localhost:5432/test')
engine = create_engine('postgresql+psycopg2://bedi:postgis4101081@localhost:5432/test')

### alternative way of connecting
# conn = psycopg2.connect(dbname="test", user="bedi", password="postgis4101081")
# cur = conn.cursor()
# sql_create table = "CREATE TABLE (%layer) IN SCHEMA test_fgdb"
# sql_insert = "INSERT INTO %layer


### list files in folder, for now it's .gdb,
### must be changed to zips including temporarily writing extracted zips to either 'geospatialdata' or a temp-folder on scratch

path_to_zip = YOURPATH_TO_ZIP

xlist_files_zip = glob.glob(path_to_zip + '*.zip')
print(xlist_files_zip)

path_to_scratch = "/home/bedi/data/fkb_fgdb/scratch/"


for i_zip in xlist_files_zip:
    # Create a ZipFile Object and load sample.zip in it
    with ZipFile(i_zip) as zipObj:
        # Extract all the contents of zip file in current directory
        zipObj.extractall(path=path_to_scratch)


    # Create a ZipFile Object and load sample.zip in it
    #with ZipFile('sampleDir.zip', 'r') as zipObj:
    # Extract all the contents of zip file in current directory
    #    zipObj.extractall()

    ### list files with ending .gdb
    xlist_files = glob.glob(path_to_scratch + '*.gdb')


    ### loop over files
    for gdb_file in xlist_files:

        ### read file
        # gdb_file = "/home/bedi/data/n50_fgdb/Basisdata_02_Akershus_25833_N50Kartdata_FGDB/Basisdata_02_Akershus_25833_N50Kartdata_FGDB.gdb"
        # gdb_file = "/home/bedi/data/fkb_fgdb/Basisdata_05_Oppland_5972_FKB-Arealbruk_FGDB.gdb"
        # print(gdb_file)

        # Get all the layers from the .gdb file
        layers = fiona.listlayers(gdb_file)
        # print(layers)

        for k, layer in enumerate(layers):

            #print(k)
            #print(layer)
            #print(gdb_file)
            geodataframe = gpd.read_file(gdb_file, layer=layer)
            xcrs = int(list(dict.values(geodataframe.crs))[0].split("epsg:")[1])
            # print(xcrs)
            print(geodataframe)

            ### get geom type for writing to postgres later
            xname = geodataframe.geom_type[0]
            # print(xname)

            ### add column 'location' indicatin fylke/kommune
            ### split filenames
            xlocation = gdb_file.split("_")[3]
            # print(xlocation)

            ### add location column
            # geodataframe.assign(location=xlocation)
            geodataframe['location'] = [xlocation] * len(geodataframe)
            # print(geodataframe.columns)

            ### make it WKT
            geodataframe['geom'] = geodataframe['geometry'].apply(lambda x: WKTElement(x.wkt, srid=xcrs))

            # drop the geometry column as it is now duplicative
            geodataframe.drop('geometry', 1, inplace=True)

            ### make date columns a date
            datecolumns = geodataframe.filter(regex='dato$').columns

            # change text to date
            if len(datecolumns) > 0:
                for i in datecolumns:
                    geodataframe[i] = pd.to_datetime(geodataframe[i],
                                                     infer_datetime_format=True,
                                                     yearfirst=True,
                                                     errors='ignore')
                    print(geodataframe.columns)

            ### to_sql --> set replace, remember that in Postgres tables must be dropped using SQL via engine
            if k == 0:
                sql = 'DROP TABLE IF EXISTS ' + layer + ' CASCADE;'
                engine.execute(sql)
                xreplace = 'replace'
                # print(xreplace)
            else:
                xreplace = 'append'

            # Use 'dtype' to specify column's type
            # For the geom column, we will use GeoAlchemy's type 'Geometry'
            geodataframe.to_sql(layer,
                                engine,
                                if_exists=xreplace,
                                index=False,
                                schema="test_fgdb_fkb",
                                dtype={'geom': Geometry(xname + 'Z',
                                                        dimension=3,
                                                        srid=xcrs)
                                       }
                                )
            print("done!")

    dir_to_remove = glob.glob("/home/bedi/data/fkb_fgdb/scratch/" + '*.gdb')
    print(str(dir_to_remove[0]))
    shutil.rmtree(str(dir_to_remove[0]))
