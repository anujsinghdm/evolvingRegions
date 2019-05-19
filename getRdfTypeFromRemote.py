import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess


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
    retryFilePath = hourlyChangedClass.replace('.nt', 'retry.nt')
    retryFile = open(retryFilePath,"a")
    changedClassHourDict = {}

    if os.path.isfile(hourlyChangedClass):
        changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)
    else:
        changedClassHourDict.clear();

    #instatitechangedClassHourDict(hourlyChangedClass)

    #
    # addedClassHourDict = {}
    # if os.path.isfile(addedlogFilePath):
    #     addedClassHourDict = instatitechangedClassHourDict(addedlogFilePath)
    # else:
    #     print ("Here added dict")
    #     addedClassHourDict.clear();
    #
    # deletedClassHourDict = {}
    # if os.path.isfile(deletedlogFilePath):
    #     deletedClassHourDict = instatitechangedClassHourDict(deletedlogFilePath)
    # else:
    #     print ("Here deleted dict")
    #     deletedClassHourDict.clear();
    #
    file = Graph()
    file.parse(filePath, format="nt")
    unique_sub = {}
    sparqlEndpoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
    for sub,pre,obj in file:
        if sub in unique_sub:
            unique_sub[sub] = unique_sub[sub] + 1
        else:
            unique_sub[sub] = 1
    for s in unique_sub:
        print (s)
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?extractedType
        WHERE {
        OPTIONAL {<""" + s.strip() + """> rdf:type ?type.}
        OPTIONAL{ <""" + s.strip() + """> <http://dbpedia.org/ontology/wikiPageRedirects> ?redirected.
        ?redirected rdf:type ?type1.}
        BIND(COALESCE(?type1, ?type) AS ?rdfType)
        BIND(COALESCE(?rdfType, "http://exp.er.unknown") AS ?extractedType)
        }"""

        checkWithInCurrentFileQuery = """
        SELECT DISTINCT ?type
        WHERE {
         <""" + s.strip() + """> rdf:type ?type.
        }
        """
        resultFromCurrentFile = file.query(checkWithInCurrentFileQuery)

        try:
            sparqlEndpoint.setQuery(query)
            sparqlEndpoint.setMethod(GET)
            sparqlEndpoint.setReturnFormat(JSON)
            results = sparqlEndpoint.query().convert()
        except Exception as e:
            print ("retrying..." + str(e))
            retryFile.write('<' + s.strip() + '>\n')
            time.sleep(1)
            continue
        #updating hour dict
        for result in results["results"]["bindings"]:
            key = "<" + result["extractedType"]["value"] + ">"
            if key == '''<http://exp.er.unknown>''':
                if len(resultFromCurrentFile.bindings) == 0:
                    if key in changedClassHourDict:
                        changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + unique_sub[s]
                    else:
                        changedClassHourDict[key] = unique_sub[s]
                else:
                    for key2 in resultFromCurrentFile["results"]["bindings"]:
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

    changedClassTS = open(hourlyChangedClass,"w+")

    for eachKeyHr in changedClassHourDict.keys():
        changedClassTS.write((eachKeyHr + " <http://er/c> " + str(changedClassHourDict[eachKeyHr]) + " . \n"))
    changedClassTS.close()
    retryFile.close()
    
    # Move a file by renaming it's path
    os.rename(filePath, filePathMoved)
    time.sleep(1)
    lock.release()
    print ("lock released on directory " + path)
