import glob
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from datetime import datetime
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import concat, lit

def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.11")\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .enableHiveSupport()\
        .getOrCreate()
    return spark

def process_imm(spark,in_path,out_path):
    #df_spark =spark.read.format('com.github.saurfang.sas.spark').load("s3a://capstone-inpath/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")
    
    sc = spark.sparkContext
    myPath = f's3://capstone-inpath/18-83510-I94-Data-2016/'
    javaPath = sc._jvm.java.net.URI.create(myPath)
    hadoopPath = sc._jvm.org.apache.hadoop.fs.Path(myPath)
    hadoopFileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem.get(javaPath, sc._jvm.org.apache.hadoop.conf.Configuration())
    iterator = hadoopFileSystem.listFiles(hadoopPath, True)

    s3_keys = []
    while iterator.hasNext():
        s3_keys.append(iterator.next().getPath().toUri().getRawPath())        
    
    for i,f in enumerate(s3_keys):
        df = spark.read.format('com.github.saurfang.sas.spark').load(in_path+f)
        if i==0 :
            df_spark=df
            col_names=df.columns
        else:
            df_spark=df_spark.union(df[col_names])

    
    cols_to_drop=["visapost","occup","entdepu","insnum"]
    df_spark=df_spark.drop(*cols_to_drop)
    df_spark=df_spark.dropna(subset=["airline","gender","matflag","entdepd","i94addr","depdate"])
    
    df_spark_valid=df_spark.select("cicid","i94port","i94mon","arrdate","depdate","i94mode", "visatype","count")
    
    df_spark_valid=df_spark_valid.withColumn('new_arrival_date', expr("date_add('1960-1-1', arrdate)"))
    df_spark_valid=df_spark_valid.withColumn('new_departure_date', expr("date_add('1960-1-1', depdate)"))
    
    df_imm=df_spark_valid.filter(df_spark_valid["i94mode"]==1.0).drop("arrdate","depdate","i94mode")
    
    #write
    df_imm.write.partitionBy('i94port','i94mon').mode('overwrite').parquet(os.path.join(out_path,"I94_immigration_table"))
    
    #people table
    df_ppl=df_spark.select("cicid","i94res", "i94cit", "gender","i94bir","biryear","i94addr")
    
    #write
    df_ppl.write.partitionBy('gender').mode('overwrite').parquet(os.path.join(out_path,"I94_People_table"))
    
    #Visa table
    df_visa=df_spark.select("visatype", "i94visa").dropDuplicates()
    
    #write
    df_visa.write.mode('overwrite').parquet(os.path.join(out_path,"Visa_table"))
    
    #time table
    df_time=df_imm.select("new_arrival_date").dropDuplicates()\
             .withColumn('Day', dayofmonth('new_arrival_date'))\
             .withColumn('Month', month('new_arrival_date'))\
             .withColumn('Year', year('new_arrival_date')).withColumnRenamed("new_arrival_date","Date")
    
    #write
    df_time.write.partitionBy('Year','Month').mode('overwrite').parquet(os.path.join(out_path,"I94_Date_table"))

def process_codes(spark,in_path,out_path):
    
    path_codes =os.path.join(in_path,"airport-codes_csv.csv")
    
    df_spark_codes=spark.read.csv(path_codes, header=True)
    df_codes=df_spark_codes.select("type","name","iso_region","iata_code")\
                    .filter((df_spark_codes["iata_code"]!="null") & (df_spark_codes["iso_country"]=="US"))\
                    .filter((df_spark_codes['type']== "large_airport"))\
                    .drop("iso_country", "type")
    
    #write to parquet files
    df_codes.write.mode('overwrite').parquet(os.path.join(out_path,"AirportCodes"))

def process_state_demographics(spark,in_path,out_path):
    
    path_cities = os.path.join(in_path,"us-cities-demographics.csv")
    
    df_spark_cities=spark.read.option("delimiter",";").csv(path_cities, header=True)
    
    df_spark_cities=df_spark_cities.withColumn("Median Age",df_spark_cities['Median Age'].cast('int'))
    df_spark_cities=df_spark_cities.withColumn("Total Population",df_spark_cities['Total Population'].cast('int'))
    
    df_states=df_spark_cities.select("City","Median Age","State Code", "Total Population").dropDuplicates()\
                     .groupby("State Code").agg(f.sum("Total Population").alias("Total_Population"), \
                                                f.avg("Median Age").alias("Median_Age") )\
                     .withColumn("new", concat(lit("US-"),df_spark_cities['State Code']))\
                     .drop("State Code").withColumnRenamed("new","State_Code")

    #write to parquet files
    df_states.write.mode('overwrite').parquet(os.path.join(out_path,"US_States_demographics"))


def main():
    spark = create_spark_session()
    input_data = "s3a://capstone-inpath/"
    output_data = "s3a://capstone-outpath1/"
    
    process_imm(spark, input_data, output_data)    
    process_codes(spark, input_data, output_data)
    process_state_demographics(spark, input_data, output_data)


if __name__ == "__main__":
    main()
