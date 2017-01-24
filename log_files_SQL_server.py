# -*- coding: utf-8 -*-
"""
Created on Tue May 24 10:13:32 2016

@author: JLLU

Record log file stats in SQL server
"""

import pandas as pd
import numpy as np
import os
import zipfile
import fnmatch
import multiprocessing as mp
import time
import pyodbc
import datetime

from scrape_proservice_dashboard import usmserver_string 
from scrape_proservice_dashboard import USMLog_string 
from scrape_proservice_dashboard import UILOG_string 
from scrape_proservice_dashboard import DMLog_string
from scrape_proservice_dashboard import IDxH800_string
from scrape_proservice_dashboard import SMSS_pt_shell_string
from scrape_proservice_dashboard import SMSS_trace_string
from scrape_proservice_dashboard import cleaned_filename_length

#%% subfunctions
def datetime_in_log_filename( filename, source_log_type ):
    if source_log_type == usmserver_string:
        a = filename.split( '.' )
        b = a[2].split('-')
        c = a[3]
        returned_datetime = datetime.datetime(int(b[0]), int(b[1]), int(b[2]), int(c[0:2]), int(c[2:4]), int(c[4:]))
    elif source_log_type == USMLog_string:
        a = filename.split( '.' )
        b = a[0].split('_')
        date_string = b[1]
        time_string = b[2]
        returned_datetime = datetime.datetime( int('20'+date_string[:2]), int(date_string[2:4]), int(date_string[4:6]), int(time_string[:2]), int(time_string[2:4]), int(time_string[4:6]))
    elif source_log_type == UILOG_string:
        a = filename.split( '.' )
        b = a[1].split('-')
        c = a[2].split('-')
        returned_datetime = datetime.datetime(int(b[0]), int(b[1]), int(b[2]), int(c[0]), int(c[1]), int(c[2]))
    elif source_log_type == DMLog_string:
        a = filename
        returned_datetime = datetime.datetime( int(a[15:19]), int(a[19:21]), int(a[21:23]), int(a[24:26]), int(a[26:28]), 0)   
    elif source_log_type == IDxH800_string:
        a = filename
        returned_datetime = datetime.datetime( int(a[16:20]), int(a[21:23]), int(a[24:26]), int(a[27:29]), int(a[30:32]), int(a[33:35]))  
    elif source_log_type == SMSS_pt_shell_string:
        a = filename
        returned_datetime = datetime.datetime( int(a[21:25]), int(a[26:28]), int(a[29:31]), int(a[32:34]), int(a[35:37]), int(a[38:40]))  
    elif source_log_type == SMSS_trace_string:
        a = filename
        returned_datetime = datetime.datetime( int(a[15:19]), int(a[20:22]), int(a[23:25]), int(a[26:28]), int(a[29:31]), int(a[32:34]))  
    else:
        raise Exception('adfaf')
        
    return returned_datetime
   
    
def full_file_stats( zip_fullpath ):
    # Get compress file size and uncompress size
    split_string = zip_fullpath.split('\\')
    instance_no = split_string[-2]
    zip_filename = split_string[-1]
    

    if zip_filename[:6] == USMLog_string:
        source_log_type = USMLog_string
    elif zip_filename[:6] == UILOG_string:
        source_log_type = UILOG_string
    elif zip_filename[:9] == usmserver_string:
        source_log_type = usmserver_string
    elif zip_filename[:14] == DMLog_string:
        source_log_type = DMLog_string
    elif zip_filename[:1] == IDxH800_string:
        source_log_type = IDxH800_string
    elif zip_filename[:16] == SMSS_pt_shell_string:
        source_log_type = SMSS_pt_shell_string
    elif zip_filename[:14] == SMSS_trace_string:
        source_log_type = SMSS_trace_string        
    else:
        source_log_type = 'Unknown'
        log_filename = ''
        log_datetime = None
        
    if source_log_type != 'Unknown':
        log_filename = zip_filename[:cleaned_filename_length[source_log_type]]
        log_datetime = datetime_in_log_filename(log_filename, source_log_type)

    zip_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(zip_fullpath))
    zip_modification_time = datetime.datetime.fromtimestamp(os.path.getmtime(zip_fullpath))
    
    compressed_size = 0
    uncompressed_size = 0   
    line_count = 0
    # os.stat('somefile.txt').st_size   # filesize
    
    try:
        with zipfile.ZipFile(zip_fullpath, 'r') as z:
            for file_in_zip in z.infolist():
                compressed_size += file_in_zip.compress_size
                uncompressed_size += file_in_zip.file_size
            with z.open( z.namelist()[0]) as f: # assume the first file in zip is the log file to read
                #temp = f.readlines()
                line_count = len(f.readlines())
    except:
        #os.remove(zip_file_path)
        pass
    
    return {'Source log type': source_log_type,
            'Instance No': instance_no,
            'Zip filename': zip_filename,
            'Log filename': log_filename,
            'Compressed size': compressed_size, 
            'Uncompressed size': uncompressed_size, 
            'Line count': line_count,
            'Zip creation datetime': zip_creation_time,
            'Zip modification datetime': zip_modification_time,
            'Log datetime': log_datetime}            
                        
            
def record_one_zip_folder( connection, cursor, zip_folder, existing_file_stats ):
    # Record all log files in a folder into SQL Server
    # Will ignore the log files that already exist

    # Use log_filename instead of zip_filename to check duplication

    # Log files in SQL
    split_string = zip_folder.split('\\')
    instance_no = split_string[-1]    
    subset = existing_file_stats.ix[ existing_file_stats['INSTANCE_NO'] == instance_no, ['ZIP_FILENAME', 'LOG_FILENAME' ] ]    
    log_filenames_in_SQL = list(subset['LOG_FILENAME'])    

    # Log files in storage 
    zip_log_filenames_in_storage_dict = {}
    for zip_filename in os.listdir( zip_folder ):
        if zip_filename.endswith('.zip'): 
            if zip_filename.startswith( usmserver_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[usmserver_string]]] = zip_filename
            elif zip_filename.startswith( USMLog_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[USMLog_string]]] = zip_filename
            elif zip_filename.startswith( UILOG_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[UILOG_string]]] = zip_filename
            elif zip_filename.startswith( DMLog_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[DMLog_string]]] = zip_filename                
            elif zip_filename.startswith( IDxH800_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[IDxH800_string]]] = zip_filename       
            elif zip_filename.startswith( SMSS_pt_shell_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[SMSS_pt_shell_string]]] = zip_filename 
            elif zip_filename.startswith( SMSS_trace_string ):
                # log_filename is key, zip_filename is value. 
                zip_log_filenames_in_storage_dict[zip_filename[:cleaned_filename_length[SMSS_trace_string]]] = zip_filename 
                
    # Matching
    # in storage but not in SQL
    zip_fullpath_pool = []
    num_storage_not_in_SQL = 0
    num_storage_in_SQL = 0
    for log_filename, zip_filename in zip_log_filenames_in_storage_dict.items():
        if log_filename in log_filenames_in_SQL:
            num_storage_in_SQL +=1
        else:
            num_storage_not_in_SQL += 1
            zip_fullpath_pool.append( os.path.join(zip_folder, zip_filename))

    num_expected_inserts = num_storage_not_in_SQL
    
    try:
        print '----------------------------'
        print '  INSTNACE NO: ', instance_no            
        print '  Number of log files in storage:', str(len(zip_log_filenames_in_storage_dict.keys()))            
        print '  Number of log files in SQL    :', str(len(log_filenames_in_SQL))
        print '  Number of log files in storage but not in SQL:', str(num_expected_inserts)
        print '  Number of log files in storage and in SQL    :', str(num_storage_in_SQL)
    except:
        pass
    
    # INSERT
    num_successful_inserts = 0
    for zip_fullpath in zip_fullpath_pool:
        error_message = record_one_log_file(connection, cursor, zip_fullpath)
        if error_message == 'No errors':
            num_successful_inserts += 1
        
    try:
        print '  Number of successful inserts:', str(num_successful_inserts)
    except:
        pass
    
    return (num_successful_inserts, num_expected_inserts)
               
            
def record_one_log_file( connection, cursor, zip_fullpath ):
    # Record the stats of one log file into SQL Server

    try:
        stats = full_file_stats( zip_fullpath )
    
        if stats['Source log type'] in [usmserver_string, USMLog_string, UILOG_string, DMLog_string, IDxH800_string, SMSS_pt_shell_string, SMSS_trace_string]:
            SQL_query = 'INSERT INTO dbo.LOG_FILE_STATS VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            
            error_message = 'No errors'
            try:
                #TODO: need to check if (instance_no, log_filename) pair has already exist
            
                cursor.execute( SQL_query, stats['Source log type'], 
                                stats['Instance No'], stats['Zip filename'], stats['Log filename'], 
                                stats['Compressed size'], stats['Uncompressed size'], stats['Line count'],
                                stats['Zip creation datetime'], stats['Zip modification datetime'],
                                stats['Log datetime'])
                
                # connection.commit() can be perform right after each individual insert or after a bunch of insert
                # In terms of execution time, both approaches are same
                connection.commit()
            except:
                error_message = 'Query errors'
        else:
            error_message = 'Unknown log type'      
    except:            
        # SMS Trace retrieved manually from the embedded CPU will cause exception
        error_message = 'Unexpected log filename'
        
    return error_message
   
           
def generate_log_stats():
    connection = pyodbc.connect('DSN=SQL Server DXH_LOGS;UID=;PWD=')
    cursor = connection.cursor()

        
           
#%% Main
if __name__ == '__main__':
    
    '''
    SQL quest to get stats:
    1) Total stats
    select count(distinct(a.instance_no)) as NUM_INSTRUMENT, 
    count(log_filename) as NUM_LOG_FILE,
    sum(a.COMPRESSED_SIZE)/1e12 as COMPRESSED_SIZE_TB,
    sum(a.UNCOMPRESSED_SIZE)/1e12 as UNCOMPRESSED_SIZE_TB,
    sum(a.LINE_COUNT)/1e9 as LINE_COUNT_billion
    from dbo.LOG_FILE_STATS a
    
    2) By-group stats
    select SOURCE_LOG_TYPE, count(distinct(a.instance_no)) as NUM_INSTRUMENT, 
    count(log_filename) as NUM_LOG_FILE,
    sum(a.COMPRESSED_SIZE)/1e12 as COMPRESSED_SIZE_TB,
    sum(a.UNCOMPRESSED_SIZE)/1e12 as UNCOMPRESSED_SIZE_TB,
    sum(a.LINE_COUNT)/1e9 as LINE_COUNT_billion
    from dbo.LOG_FILE_STATS a
    group by SOURCE_LOG_TYPE  
    order by source_log_type
    
    select *
    from dbo.LOG_FILE_STATS
    order by LOG_DATETIME desc
    
    select *
    from RETRIEVAL_HISTORY
    order by RETRIEVAL_DATETIME desc
    
    3) Clear all content in the table
    truncate table dbo.LOG_FILE_STATS
    
    4) Get instrument-day measure per log type
    select b.source_log_type, count(*) as num_instance, sum(b.Duration) as instrument_day, sum(b.Duration) /count(*) as days_per_instance
	from (
	select a.INSTANCE_NO as instance_no, a.SOURCE_LOG_TYPE as source_log_type, count(*) as item_count, 
			datediff(day, min(a.log_datetime), max(a.log_datetime)) as Duration, 
			max(a.log_datetime) as min_date, min(a.log_datetime) as max_date
    from dbo.LOG_FILE_STATS a
	group by a.INSTANCE_NO, a.SOURCE_LOG_TYPE ) b
	group by b.source_log_type
 
     5) Find missing SMSS Trace logs
     select count(*)
     from dbo.LOG_FILE_STATS
     where SOURCE_LOG_TYPE = 'SMSS-Sms-Trace'

    select count(*)
    from dbo.LOG_FILE_STATS
    where SOURCE_LOG_TYPE = 'SMSS-Sms-Trace' and UNCOMPRESSED_SIZE < 1000     
    '''    
    
    root_folder = r'\\global\miami-rd\Projects\Reliability\DxH\DeveloperLogs\logs'
    #root_foler = r'D:\usm'
    num_worker = 16

    connection = pyodbc.connect('DSN=SQL Server DXH_LOGS;UID=;PWD=')
    cursor = connection.cursor()
    
    cycle_count = 1
    total_start = time.time()
    accumulated_num_successful_inserts_total = 0
    accumulated_num_expected_inserts_total = 0
    while True:
        cycle_start = time.time()
        accumulated_num_successful_inserts_in_cycle = 0
        accumulated_num_expected_inserts_in_cycle = 0
        # Collect all record in LOG_FILE_STATS table
        cursor.execute( 'select INSTANCE_NO, ZIP_FILENAME, LOG_FILENAME from dbo.LOG_FILE_STATS') 
        
        while True:
            try:
                rows = cursor.fetchall()
                break
            except:
                time.sleep(10)    

        column_name = [x[0] for x in cursor.description]
        existing_file_stats = pd.DataFrame.from_records( rows, columns = column_name)
        existing_file_stats.index = [''] * len(existing_file_stats)
        #connection.close()
        print 'Finished querying LOG_FILE_STATS in SQL_Server'

        # Go through each zip folder. 
        all_zip_folder = os.listdir(root_folder)
        
        # For debugging
        #all_zip_folder = [r'R:\Projects\Reliability\DxH\DeveloperLogs\logs\6774889']
        
        folder_counter = 0
        problematic_folder_pool = []
        for temp_zip_folder in all_zip_folder:
            num_successful_inserts, num_expected_inserts = \
                record_one_zip_folder( connection, cursor, os.path.join(root_folder, temp_zip_folder), existing_file_stats )
            folder_counter += 1
            if num_successful_inserts != num_expected_inserts:
                problematic_folder_pool.append(temp_zip_folder)

            accumulated_num_successful_inserts_in_cycle += num_successful_inserts
            accumulated_num_successful_inserts_total    += num_successful_inserts
            accumulated_num_expected_inserts_in_cycle += num_expected_inserts
            accumulated_num_expected_inserts_total    += num_expected_inserts
            
            try:
                print '  Finished ', str(folder_counter), ' folders out of ', str(len(all_zip_folder))
                print '  Cycle count:', cycle_count
                print '  Number of accumulated successful inserts in current cycle: ', str(accumulated_num_successful_inserts_in_cycle)
                print '  Number of accumulated EXPECTED inserts in current cycle: '  , str(accumulated_num_expected_inserts_in_cycle)
                print '  Number of accumulated successful inserts total           : ', str(accumulated_num_successful_inserts_total)
                print '  Number of accumulated EXPECTED inserts total           : '  , str(accumulated_num_expected_inserts_total)  
                print '  Elapsed seconds in current cycle:', str(time.time()-cycle_start)                
                print '  Elapsed seconds total           :', str(time.time()-total_start)
                
                if accumulated_num_successful_inserts_total != accumulated_num_expected_inserts_total:
                    print '  ------- Error found !!! -------'
                    if len(problematic_folder_pool) <= 20:
                        print '  problematic_folder_pool = '
                        print problematic_folder_pool
                    else:
                        print '  Check problematic_folder_pool !!!'                        
            except:
                pass

        cycle_count += 1
        
        if accumulated_num_successful_inserts_in_cycle < 300:
            print 'Sleeping ...'
            time.sleep(600) # wait 1800 seconds. 
            print 'Waking up ...'
            
    raise Exception('To delete the following code')



    '''
    # Collect all files in storage
    zip_fullpath_pool = []
    counter = 0
    start = time.time()
    for root, dirnames, filenames in os.walk(root_folder):
        for filename in fnmatch.filter(filenames, '*.zip'):
            zip_fullpath_pool.append( os.path.join( root, filename ))
            
            counter += 1
            if (counter%1000) == 0:
                try:
                    print counter
                except:
                    pass    
    print 'Finished gathering all zip file paths'            
    print time.time() - start
    
    # INSERT to SQL Server
    error_file_path = []
    counter = 0
    for zip_fullpath in zip_fullpath_pool:
        try:
            error_message = record_one_log_file(connection, cursor, zip_fullpath)
            
            if error_message != 'No errors':
                print os.path.join(root, filename)
                error_file_path.append( os.path.join(root, filename))
        except:
            # Reopen a connection to SQL Server
            connection.close()
            connection = pyodbc.connect('DSN=SQL Server DXH_LOGS;UID=;PWD=')
            cursor = connection.cursor()
    
        counter += 1
        if counter%100 == 0:
            try:
                print counter
            except:
                pass
    '''            
    
    
    # Abandoned method #2: Generate file stats    
    '''
    start_time = time.time()
    if False:
        # parallel
        print 'Number of Workers = ', num_worker     
        pool = mp.Pool(processes = num_worker)
        all_fill_stats_list = pool.map(file_stats, zip_fullpath_pool)
        pool.close()
        pool.join()    
    else:
        # Sequential
        print 'Sequential run'
        line_counter = 0
        all_fill_stats_list = []
        for line in zip_fullpath_pool:
            # Go though each line
            
            # May contain blank element in all_fill_stats
            all_fill_stats_list.append( file_stats(line) )
                    
            line_counter += 1            
            if (line_counter % 10000) == 0:
                try:
                    print line_counter
                except:
                    pass                         
                             
    all_fill_stats_df = pd.DataFrame( filter(None, all_fill_stats_list))            
    all_fill_stats_df.index = [''] * len(all_fill_stats_df)
    print time.time() - start_time      
    '''
    
    
    
    '''
    # List all files recursively
    error_file_path = []
    count = 0
    for root, dirnames, filenames in os.walk(root_folder):
        for filename in fnmatch.filter(filenames, '*.zip'):
            try:
                error_message = record_one_log_file(connection, cursor, root, filename)
            
                if error_message != 'No errors':
                    print os.path.join(root, filename)
                    error_file_path.append( os.path.join(root, filename))
            except:
                # Reopen a connection to SQL Server
                connection.close()
                connection = pyodbc.connect('DSN=SQL Server DXH_LOGS;UID=;PWD=')
                cursor = connection.cursor()
    
            count += 1
            if count%100 == 0:
                raise Exception('adfa')
                try:
                    print count
                except:
                    pass
    '''
            
    
