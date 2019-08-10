from multiprocessing import Pool, TimeoutError
import multiprocessing
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
        if len(files) > 0:
            for eachFile in files:
                if ".nt" in eachFile:
                    allFilePath.append(eachFile)
    return allFilePath

day = "31"
path = '../../DBpediaChangeSet/' + day
if __name__ == '__main__':
    pool = Pool(1)
    pool.map(getRdfTypeFromRemote.getClasses, get_immediate_subdirectories(path))
    pool.close()
    pool.join()
