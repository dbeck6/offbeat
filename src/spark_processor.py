import argparse
import hnswlib
import numpy as np
import os
import sys

from pyspark import SparkFiles
from pyspark.sql import Row
from pyspark.sql import SparkSession

from msd import MSDInterface
from postgres import PostgresConnector
from s3 import S3Interface
from spotify import SpotifyInterface
from vector import vector_processor

from pyspark import SparkFiles
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import row_number


class SparkProcessor:
    '''
    Distributes songs derived from  Million Song Dataset or Spotify API for processing
    across Spark cluster to extract features that will be written to Postgres DB.
    '''
    def __init__(self, data_source, num_songs, songs_offset=0, folder='', playback=''):

        self.num_songs = num_songs
        self.songs_offset = songs_offset

        self.data_source = data_source
        self.folder = folder
        self.fetcher = S3Interface()
        self.playback = playback

        self.spark = SparkSession.builder.appName('Offbeat').getOrCreate()
        self.spark.sparkContext.addPyFile('vector.py')

        self.spark.conf.set('spark.dynamicAllocation.enable', 'true')
        self.spark.conf.set('spark.dynamicAllocation.executorIdleTimeout', '2m')
        self.spark.conf.set('spark.dynamicAllocation.minExecutors', '1')
        self.spark.conf.set('spark.dynamicAllocation.maxExecutors', '2000')
        self.spark.conf.set('spark.stage.maxConsecutiveAttempts', '10')
        self.spark.conf.set('spark.memory.offHeap.enable', 'true')
        self.spark.conf.set('spark.memory.offheap.size', '3g')
        self.spark.conf.set('spark.executor.memory', '5g')
        self.spark.conf.set('spark.driver.memory', '5g')

        self.db_writer = PostgresConnector()

        if (self.data_source == 'msd'):
            # grabbing from file path on s3 and downloading to df
            #song_data_df = self.fetcher.get_keys(folder)
            #self.fetcher.download_files(song_data_df)
            self.interface = MSDInterface()
        elif (self.data_source == 'spotify'):
            self.interface = SpotifyInterface()

    def run_processing(self):

        if (self.data_source == 'msd'):
            song_data_list = self.interface.get_music(num_songs=self.num_songs, offset=self.songs_offset)
        elif (self.data_source == 'spotify'):
            song_data_list = self.interface.get_music(num_songs=self.num_songs, offset=self.songs_offset, playback=self.playback)
        song_data_df = self.spark.createDataFrame(Row(**song_dict) for song_dict in song_data_list)

        # process df to retrieve music information
        song_info_df = song_data_df.select('id', 'source_id', 'name', 'artist', 'year', 'popularity', 'url')
        song_info_df = song_info_df.withColumn('source', lit(self.data_source))

        # build song vector df
        comp_vec_udf = udf(vector_processor(method='gauss'), returnType=ArrayType(DoubleType()))
        song_vec_df = song_data_df.withColumn('vector', comp_vec_udf('timbre', 'chroma'))
        song_vec_df = song_vec_df.select('id', 'vector')
        song_vec_df = song_vec_df.withColumn('method', lit('gauss'))

        # write vectors to the similarity search index
        self.write_to_hnswlib(song_vec_df)

        # write dfs to db
        self.db_writer.write(song_info_df, 'song_info', mode='append')
        self.db_writer.write(song_vec_df, 'song_vectors', mode='append')

    def write_to_hnswlib(self, vec_df):

        index_filename = 'index.bin'
        num_elements = 2000000
        sample_vec = vec_df.limit(1).collect()[0].vector
        dim = len(sample_vec)

        if (os.path.isfile(index_filename)):
            # if index exists, reinitate and load
            index = hnswlib.Index(space = 'cosine', dim = dim)
            index.load_index(index_filename, max_elements = num_elements)
            index.set_ef(50)
        else:
            index = hnswlib.Index(space = 'cosine', dim = dim)
            index.init_index(max_elements = num_elements, ef_construction = 100, M = 64)
            index.set_ef(50)

        vec_table = vec_df.select('id', 'vector').collect()
        ids_list = [row.id for row in vec_table]
        vecs_list = [row.vector for row in vec_table]
        ids_arr = np.array(ids_list, copy=False, dtype=np.int64)
        vecs_arr = np.array(vecs_list, copy=False, dtype=np.float32)

        index.add_items(vecs_arr, ids_arr)
        # Query the elements for themselves and measure recall:
        labels, distances = index.knn_query(vecs_arr, k=1)
        print("Recall for this batch:", np.mean(labels.reshape(-1) == ids_arr.reshape(-1)), "\n")
        index.save_index(index_filename)

def get_parser():

    parser = argparse.ArgumentParser(
        description='Processes songs retrieved from either MSD or Spotify'
    )
    parser.add_argument('-n', '--number', 
        help='Specify the number of songs to retrieve from the data source', 
        required=True, type=int
    )
    parser.add_argument('-o', '--offset',
        help='Specify the offset from the beginning of the list of songs',
        default=0, type=int)
    parser.add_argument('-f', '--folder',
        help='Specify the s3 folder of the list of songs',
        default='', type=str
    )
    parser.add_argument('-s', '--source',  
        help='Select either "msd" or "spotify" as the data source', 
        choices=['msd', 'spotify'],
        required=True, type=str
    )
    parser.add_argument('-p', '--playback',
        help='Select either "recent" or "playlist" as the listening source',
        default='', type=str
    )

    return parser

def main():

    parser = get_parser()
    args = parser.parse_args()
    spark_processor = SparkProcessor(num_songs=args.number, 
                                songs_offset=args.offset, 
                                data_source=args.source, 
                                folder=args.folder,
                                playback=args.playback)
    spark_processor.run_processing()

if (__name__ == '__main__'):

    main()
