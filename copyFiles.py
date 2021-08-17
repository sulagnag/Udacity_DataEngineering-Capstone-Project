import boto3
import os
import configparser
import glob

config = configparser.ConfigParser()
config.read('config.cfg')

location = config['AWS']['REGION_NAME']
bucket1_name="capstone-inpath"
bucket2_name="capstone-outpath"

session = boto3.Session(
    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
)
s3 = session.resource('s3')

    
s3.create_bucket(Bucket=bucket1_name, CreateBucketConfiguration={\
                'LocationConstraint': 'us-west-2'})

s3.meta.client.upload_file('airport-codes_csv.csv', bucket1_name, 'airport-codes_csv.csv')
s3.meta.client.upload_file('us-cities-demographics.csv', bucket1_name, 'us-cities-demographics.csv')

in_path='../../data/18-83510-I94-Data-2016'

files=glob.glob(os.path.join(in_path,"*"))
for f in files:
    fp=os.path.split(f)
    print (fp[1])
    s3.meta.client.upload_file(f, bucket1_name, os.path.join("18-83510-I94-Data-2016",fp[1]))
    
    
s3.create_bucket(Bucket=bucket2_name, CreateBucketConfiguration={\
                'LocationConstraint': 'us-west-2'})
   