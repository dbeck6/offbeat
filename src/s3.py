import boto3
from botocore.client import Config
import botocore
import os
import pandas as pd
import re
import sys
import time


class S3Interface:
	'''
	Interface between Spark and S3 to obtain data files for processing
	'''

	def __init__(self):

		self.bucket = os.environ.get('AWS_BUCKET')
		self.region = os.environ.get('AWS_REGION')
		self.access = os.environ.get('AWS_ACCESS_KEY_ID')
		self.secret = os.environ.get('AWS_SECRET_ACCESS_KEY')

	def write_to_csv(self, df):
		df.to_csv('all_msd_files.csv', index=False)

	def download_files(self, df):
		# download hdf5 files from s3 and provide list for processing
		client = boto3.client('s3')
		df = df.apply(lambda row: client.download_file(Bucket=self.bucket, 
								Key=row['s3_object'], 
								Filename=row['file_path']),
								axis=1)
		# verify all downloads have completed
		path = '/home/ubuntu/tmp'
		count = 0
		while(count < len(df)):
			count = len([f for f in os.listdir(path)if os.path.isfile(os.path.join(path, f))])

	def get_keys(self, Prefix='', Delimiter='/'):
		start = time.time()
		Prefix = Prefix[1:] if Prefix.startswith(Delimiter) else Prefix
		if 'StartAfter' not in locals() and Prefix.endswith(Delimiter):
			StartAfter = Prefix
		del Delimiter

		df = pd.DataFrame(columns = ['s3_object', 'file_path'])

		for page in boto3.client('s3').get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=Prefix):
			for content in page.get('Contents', ()):
				df = df.append({'s3_object': content['Key'], 
						'file_path': '/home/ubuntu/tmp/{}'.format( \
						re.sub('data\/[A-Z]\/[A-Z]\/[A-Z]\/', '', content['Key']))}, 
						ignore_index = True)

		print(time.time() - start)
		return df

if (__name__ == '__main__'):
    start = time.time()
    s3 = S3Interface()
    start = time.time()
    files = s3.get_keys(Prefix='data/A/A/A')
    #s3.write_to_csv(files)
    s3.download_files(files)
    print(time.time() - start)
    #print(len(files))
