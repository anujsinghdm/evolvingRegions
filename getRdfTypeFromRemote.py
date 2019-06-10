import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess
import re
import gc

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
    sparqlEndpoint = SPARQLWrapper("http://192.168.178.39:7200/repositories/Repo01")

    for sub,pre,obj in file:
        if sub in unique_sub:
            unique_sub[sub] = unique_sub[sub] + 1
        else:
            unique_sub[sub] = 1


    changeGraph = Graph()
    changeGraph.parse(data=open("./allChanges.nt", "r").read(), format="nt")

    for s in unique_sub:
        print (s)
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?extractedType
        WHERE {
        graph <http://dbpedia/2015>
        {
        <""" + s.strip() + """> rdf:type ?extractedType.
        }
        }"""

        query1 = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?type
        WHERE {
        <""" + s.strip() + """> rdf:type ?type.
        }"""

        resultFromChanges = changeGraph.query(query1)

        try:
            #query in full data
            sparqlEndpoint.setQuery(query)
            sparqlEndpoint.setMethod(GET)
            sparqlEndpoint.setReturnFormat(JSON)
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
            if key == '''<http://exp.er.unknown>''':
                if len(resultFromChanges.bindings) == 0:
                    if key in changedClassHourDict:
                        changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + unique_sub[s]
                    else:
                        changedClassHourDict[key] = unique_sub[s]
                else:
                    for key2 in resultFromChanges.bindings:
                        key2 = "<" + str(key2["type"]) + ">"
                        if key2 in changedClassHourDict:
                            changedClassHourDict[key2] = int(str(changedClassHourDict[key2]).strip()) + unique_sub[s]
                        else:
                            changedClassHourDict[key2] = unique_sub[s]
            else:
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

    for eachKeyHr in changedClassHourDict.keys():
        changedClassTS.write((eachKeyHr + " <http://er/c> " + str(changedClassHourDict[eachKeyHr]) + " . \n"))
    changedClassTS.close()
    retryFile.close()

    # Move a file by renaming it's path
    os.rename(filePath, filePathMoved)
    del file
    gc.collect()
    time.sleep(1)
    lock.release()
    print ("lock released on directory " + path)
