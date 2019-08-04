import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess
import re
import gc
Wrapper._returnFormatSetting = ['format']

lock = threading.Lock()


def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

def instatitechangedClassHourDict (filePath):
    intialDictionary = {}
    changedClassLog = open(filePath,"r")
    for eachLine in changedClassLog:
        eachLine = eachLine.strip()
        intialDictionary[eachLine.split('<http://er/c>')[0].strip()] = eachLine.split('<http://er/c>')[1].replace('.','').strip()
    return intialDictionary


def getClasses (path):
    lock.acquire()
    print ("lock acquired on directory " + path)
    hourlyChangedClass = "../../changedClasses/" + invokeParallelProcess.day + "/"+ path
    filePath = hourlyChangedClass.replace('changedClasses', 'DBpediaChangeSet')
    print ("############################################################")
    print (filePath)
    print ("############################################################")

    retryResources = []
    changedClassHourDict = {}
    changeLogDir = hourlyChangedClass.replace(hourlyChangedClass.split("/")[len(hourlyChangedClass.split("/")) - 1],"")
    if not os.path.exists(changeLogDir):
        os.makedirs(changeLogDir)
    print (changeLogDir)
    print (hourlyChangedClass)
    if os.path.isfile(hourlyChangedClass):
        changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)
    else:
        changedClassHourDict.clear();

    tempFile = ""

    file = Graph()
    miniG = Graph()
    for eachTriple in open(filePath, "r"):
        try:
            miniG.parse(data=eachTriple,format="nt")
            tempFile = tempFile + eachTriple
        except Exception as e:
            retryResources.append(eachTriple)
            print (eachTriple)

    file.parse(data=tempFile, format="nt")
    print(len(file))
    unique_sub = {}
    sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")

    for sub,pre,obj in file:
        if sub in unique_sub:
            unique_sub[sub] = unique_sub[sub] + 1
        else:
            unique_sub[sub] = 1

    print (len(unique_sub))
    # changeGraph = Graph()
    # changeGraph.parse(data=open("./allChanges.nt", "r").read(), format="ttl")
    # print ("Graph generated")

    for counter, s in enumerate(unique_sub):
        print (s)
        print (counter)
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT distinct ?extractedType
        WHERE {
        <""" + s.strip() + """> rdf:type ?extractedType.
        }"""

        try:
            sparqlEndpoint.setMethod(GET)
            sparqlEndpoint.setReturnFormat(JSON)
            sparqlEndpoint.setQuery(query)
            results = sparqlEndpoint.query().convert()

        except Exception as e:
            print ("retrying..." + str(e))
            retryResources.append('<' + s.strip() + '>')
            time.sleep(1)
            continue

        if len(results["results"]["bindings"]) == 0:
            results = {
                          "head": {
                            "vars": [
                              "extractedType"
                            ]
                          },
                          "results": {
                            "bindings": [
                              {
                                "extractedType": {
                                  "type": "literal",
                                  "value": "http://exp.er.unknown"
                                }
                              }
                            ]
                          }
                        }

        #updating hour dict
        for result in results["results"]["bindings"]:
            key = "<" + result["extractedType"]["value"] + ">"
            if key in changedClassHourDict:
                changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + unique_sub[s]
            else:
                changedClassHourDict[key] = unique_sub[s]

    filePathMoved = filePath.replace('DBpediaChangeSet', 'DBpediaChangeSetDone')
    retryFilePath = hourlyChangedClass.replace('.nt', 'retry.nt')
    retryFile = open(retryFilePath,"w+")

    for eachToBeRetry in retryResources:
        retryFile.write(eachToBeRetry + "\n")

    changedClassTS = open(hourlyChangedClass,"w+")

    try:
        for eachKeyHr in changedClassHourDict.keys():
            changedClassTS.write((eachKeyHr + " <http://er/c> " + str(changedClassHourDict[eachKeyHr]) + " . \n"))
        changedClassTS.close()
        retryFile.close()
    except Exception as e:
        print (str(e))

    # Move a file by renaming it's path
    os.rename(filePath, filePathMoved)
    del file
    gc.collect()
    time.sleep(1)
    lock.release()
    print ("lock released on directory " + path)
