from multiprocessing import Pool, TimeoutError
import time
import os
import getRdfTypeFromRemote
import recordDBpediaCurrentState
from glob import glob

#python 2.7 compatible code
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
    pool = Pool(processes=24)              # start 24 worker processes
    pool.map(getRdfTypeFromRemote.getClasses, get_immediate_subdirectories(path))
