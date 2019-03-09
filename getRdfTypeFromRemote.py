import glob, os
from rdflib import Graph
import pprint

path = '../../DBpediaChangeSet'

def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

filePath =  getAllFilePaths(path)[1]
file = Graph()
file.parse(filePath, format="nt")

for s,p,o in file:
    pprint.pprint(str(s))
