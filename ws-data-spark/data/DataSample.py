
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,stddev,col, max, count
from rtree import index
import geopy.distance
from math import pi

#function to read file
def read_data(file):
    data = spark.read.option("header",True).csv(file)
    return data 

#function to clean up space in column names and remove duplicates
def clean_up(df,POI_df):
    df = df.withColumnRenamed(' TimeSt', 'TimeSt')
    POI_df = POI_df.withColumnRenamed(' Latitude','Latitude')
    df = df.drop_duplicates(subset=['TimeSt','Longitude','Latitude'])
    return df,POI_df

#function to assign label for each responses
def label(df, POI_df):
    idx = index.Index()
    label_list = []
    for i,row in enumerate(POI_df.rdd.toLocalIterator()):
        lat = float(row['Latitude'])
        lon = float(row['Longitude'])
        idx.insert(i, (lat, lon, lat, lon), obj=row['POIID'])

    for i,row in enumerate(df.rdd.toLocalIterator()):
        lat = float(row['Latitude'])
        lon = float(row['Longitude'])
        label = [(i.object) for i in idx.nearest((lat, lon, lat, lon), 1, objects=True)]
        label_list.append(label[0])

    pd_df = df.toPandas()
    pd_POI = POI_df.toPandas()

    pd_df['label'] = label_list
    merged_df = pd_df.merge(pd_POI,left_on='label',right_on='POIID',how='inner').drop(['POIID'],axis=1)

    return merged_df

#function to calculate distance of each response from labelled POI
def calculate_distance(merged_df):
    distance =[]
    for index, row in merged_df.iterrows():
        coord1 = (row['Latitude_x'],row['Longitude_x'])
        coord2 = (row['Latitude_y'],row['Longitude_y'])
        distance.append(geopy.distance.distance(coord1, coord2).km)


    merged_df['distance'] = distance
    return merged_df

#function to calculate mean, std, radius and density
def analysis(df):
    stats_df = df.groupBy('label').agg(stddev('distance').alias('std_distance'), mean('distance').alias('avg_distance'))

    radius_df = df.groupBy('label').agg(max('distance').alias('radius'))
    requests_df = df.groupBy('label').agg(count('_ID').alias('requests'))

    radius_requests_df = radius_df.join(requests_df,radius_df.label == requests_df.label,"inner")
    radius_requests_df = radius_requests_df.withColumn('area', pi*col('radius')**2)
    density_df = radius_requests_df.withColumn('density', col('requests')/col('area'))
    
    return stats_df, radius_df, density_df

if __name__=='__main__':
    
    spark = SparkSession.builder.appName('DataSample').getOrCreate()

    df = read_data("DataSample.csv")
    POI_df = read_data("POIList.csv")

    df, POI_df = clean_up(df,POI_df)

    merged_df = label(df, POI_df)

    merged_df = calculate_distance(merged_df)

    df = spark.createDataFrame(merged_df)

    stats_df, radius_df, density_df = analysis(df)

    stats_df.show()
    radius_df.show()
    density_df.show()