from multiprocessing import Pool, TimeoutError
import time
import os
import getRdfTypeFromRemote
from glob import glob

def get_immediate_subdirectories(dirPath):
    allFilePath = []
    count = 1;
    for subdir, dirs, files in os.walk(dirPath):
        if count == 1 :
            count = count + 1
        else :
            allFilePath.append(subdir)
    return allFilePath

path = '../../DBpediaChangeSet/01'
if __name__ == '__main__':
    pool = Pool(processes=4)              # start 24 worker processes
    pool.map(getRdfTypeFromRemote.getClasses, get_immediate_subdirectories(path))
