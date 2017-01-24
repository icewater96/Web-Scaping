# -*- coding: utf-8 -*-
"""
Created on Tue May 24 10:13:32 2016

@author: JLLU

Common functions to characterize log files. 

Note: this file is obsolete
"""

import pandas as pd
import numpy as np
import os
import zipfile
import fnmatch
import multiprocessing as mp
import time

raise Exception('This code is obsolete. Keep it here for traceability' )

#%% subfunctions
def count_lines( zip_file_path ):
    with zipfile.ZipFile(zip_file_path, 'r') as z:
        with z.open( z.namelist()[0]) as f:  # assume the first file zip is the log file to read
            result = len(f.readlines())
    return result


def file_size( zip_file_path ):
    # Get compress file size and uncompress size
    compressed_size = 0
    uncompressed_size = 0   
    # os.stat('somefile.txt').st_size   # filesize
    
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            for file_in_zip in z.infolist():
                compressed_size += file_in_zip.compress_size
                uncompressed_size += file_in_zip.file_size
    except:
        os.remove(zip_file_path)
                        
    return {'Compress size': compressed_size, 'Uncompress size': uncompressed_size}            
            
            
def delete_empty_folder( root_folder ):
    # Remove empty folder that is 1-level below the root_folder
    files = os.listdir( root_folder )
    
    counter = 0
    if len(files):
        for f in files:
            fullpath = os.path.join( root_folder, f )
            
            if os.path.isdir(fullpath):
                temp_files = os.listdir(fullpath)
                
                if len(temp_files) == 0:
                    counter += 1
                    #print fullpath
                    os.rmdir(fullpath)
                    
    return counter

#%% Main
if __name__ == '__main__':
    
    root_folder = r'\\global\miami-rd\Projects\Reliability\DxH\DeveloperLogs\logs\usm'
    #root_foler = r'D:\usm'
    num_worker = 16
    
    # List all files recursively
    all_file_path = []
    for root, dirnames, filenames in os.walk(root_folder):
        for filename in fnmatch.filter(filenames, '*.zip'):
            all_file_path.append(os.path.join(root, filename))
    print 'Total number of files', len(all_file_path)             
            
    # Read file size and delete invalid zip file
    start = time.time()
    if True: 
        # Parallel
        print 'Number of Workers = ', num_worker     
        pool_1 = mp.Pool(processes = num_worker)
        size_list = pool_1.map(file_size, all_file_path)
        pool_1.close()
        pool_1.join()    
    else:
        # Sequential
        size_list = []       
        print 'Total number of files', len(all_file_path)
        counter = 0
        for file_path in all_file_path:
            size_list.append( file_size( file_path ) )
            counter += 1
            if (counter%10000) == 0:
                print counter
    
    size_df = pd.DataFrame(size_list)
    compressed_size = size_df['Compress size'].sum()
    uncompressed_size = size_df['Uncompress size'].sum()
    print time.time() - start    
         

    # List all files recursively again after removing invalid zip files
    # This is neceesary for counting line since some files haven't deleted already
    # On the other hand, there may be new files that are just saved in the storage. 
    all_file_path = []
    for root, dirnames, filenames in os.walk(root_folder):
        for filename in fnmatch.filter(filenames, '*.zip'):
            all_file_path.append(os.path.join(root, filename))
                        
    print 'Total number of files after removing invalid files:', len(all_file_path)        
    print 'Total number of instruments   :', len( os.listdir(root_folder)) # Assume there is only one-level of child folder
    print 'Total   compressed size (byte):', compressed_size
    print 'Total   compressed size (GB)  :', compressed_size / 1e9
    print 'Total uncompressed size (byte):', uncompressed_size
    print 'Total uncompressed size (GB)  :', uncompressed_size / 1e9



    # Count lines 
    start = time.time()
    if True:
        # Parallel
        print 'Number of Workers = ', num_worker     
        pool_2 = mp.Pool(processes = num_worker)
        line_count_list = pool_2.map(count_lines, all_file_path)
        pool_2.close()
        pool_2.join()
        
        line_count = 0
        for i in line_count_list:
            line_count += i
    else:
        # Sequential
        line_count = 0    
        counter = 0
        for file_path in all_file_path:
            line_count += count_lines(file_path)
            
            counter += 1
            if (counter%100) == 0:
                print counter
    print time.time() - start
    
    print 'Total line count      :', line_count
    print 'Total line count (1e9):', line_count / 1e9    