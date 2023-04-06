# Used to convert mftecmd csv files into a unified timeline data structure stored as parquet files using Polars
# need to set a few variables for hostname, start_date_str, and csv_path
# need to check hostname extractor from csv file if file name does not contain the hostname

# Data Science Imports
import numpy as np
import pandas as pd
import polars as pl

# Standard python imports
import datetime
import re
import os
# import json
import glob
from pathlib import Path

# capture or set system name for mft origin
hostname = ""

# Optional Start Date for filesystem timeline - set or remove the start date which filters out all timestamps older than the start date 
start_date_str = ""

start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')

# output file name for parquet
mftecmd_UTL_filename = (datetime.datetime.now().strftime("%Y-%m-%d")+'_'+hostname+'_mftecmd_UTL.parquet')

# Locate mftecmd csv out and read into polars data frame
csv_path = ""
time_columns = ['Created0x10','LastAccess0x10','LastModified0x10','LastRecordChange0x10']
read_in_mftecmd_cvs = (pl.read_csv(csv_path).with_columns([
                                                    pl.lit(os.path.split(csv_path)[1]).alias('csv_file'),
                                                    pl.col(time_columns).str.strptime(pl.Datetime,fmt="%Y-%m-%d %H:%M:%S%.f",strict=False).dt.cast_time_unit(tu='ns'),
                                                        ])
                            )

# Output columns selected in function - Current columns to out put found in:
# utl_select_columns = [
#                 'datetime',
#                 'timestamp_desc',
#                 'resource_name',
#                 'message',
#                 'parser',
#                 'system',
#                 'comment',
#                 'tag',
#                 'user',
#                 'resource_type']


# mftecmd UTL df constructor per timestamp column
def mftecmd_UTL_pldf_out(mft_first_read_pldf : pl.DataFrame, start_time_ : datetime = start_date) -> pl.DataFrame:
    mftecmd_serial_col = ['EntryNumber',
                'SequenceNumber',
                'InUse',
                'ParentPath',
                'FileName',
                'Extension',
                'FileSize',
                'IsDirectory',
                'HasAds',
                'IsAds',
                'uSecZeros',
                'Copied',
                'SiFlags',
                'NameType',
                'Created0x10',
                'LastModified0x10',
                'ZoneIdContents',
                'csv_file']
    utl_select_columns = [
                'datetime',
                'timestamp_desc',
                'resource_name',
                'message',
                'parser',
                'system',
                'comment',
                'tag',
                'user',
                'resource_type']
    start_date = start_time_
    def mft_single_col_UTL_pldf(mft_pldf : pl.DataFrame, time_column : str,start_time_ : datetime.datetime = start_date,):
        if time_column == 'Created0x10':
            timedesc = 'TimeCreated'
        elif time_column == 'LastModified0x10':
            timedesc = 'TimeModified'
        else:
            print('time_column arguement does not match "Created0x10" or "LastModified0x10"')

        return (mft_pldf
            .filter( pl.col(time_column) > start_time_)
            .with_columns([
                # pl.col([time_column,'csv_file','EntryNumber','FileName']).hash(1).alias('row_id'),
            (pl.from_pandas(mft_pldf.select(pl.col(mftecmd_serial_col).filter( pl.col(time_column) > start_time_)).to_pandas().apply(lambda x: x.to_json(force_ascii=False), axis=1))).alias('message'),
                ])
            .select([ 
                pl.col(time_column).alias('datetime'),
                pl.lit(timedesc).alias('timestamp_desc'),
                pl.col('csv_file').str.extract('202\d+?_(.*)_MFTecmd_out\.csv').alias('system'), # set the host name for the MFT
                pl.col('FileName').alias('resource_name'),
                pl.lit('').alias('comment'),
                pl.lit('').alias('tag'),
                pl.lit('mftecmd').alias('parser'),
                pl.col('message'),
                pl.lit('').alias('user'),
                (pl.when(pl.col('IsDirectory') == True).then(pl.lit('file : node')).otherwise(pl.lit('file : leaf'))).alias('resource_type')
                    ])  
            )
    return (pl.concat([
                    mft_single_col_UTL_pldf(mft_pldf=mft_first_read_pldf,start_time_=start_date,time_column='Created0x10'),
                    mft_single_col_UTL_pldf(mft_pldf=mft_first_read_pldf,start_time_=start_date,time_column='LastModified0x10'),
                    ])
            .select(pl.col(utl_select_columns))        
            .sort('datetime')
            )


# Execute function and Convert pldf to parquet file 

mftecmd_UTL_pldf_out(read_in_mftecmd_cvs).write_parquet(mftecmd_UTL_filename,compression='lz4')

