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


path = '../../DBpediaChangeSet/01/00'
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
    allPreviousFilesAndCurrentFile = Graph()
    for filePath in getAllFilePaths(path):
        print ("############################################################")
        print (filePath + str(os.getpid()))
        print ("############################################################")
        hourlyChangedClass = filePath.replace('DBpediaChangeSet', 'changedClasses')

        hourlyChangedClass = hourlyChangedClass.replace('/' + hourlyChangedClass.split('/')[len(filePath.split('/')) - 1], '.ttl')
        retryFilePath = hourlyChangedClass.replace('.ttl', 'retry.ttl')
        addedlogFilePath = hourlyChangedClass.replace('.ttl', 'added.ttl')
        deletedlogFilePath = hourlyChangedClass.replace('.ttl', 'deleted.ttl')

        retryFile = open(retryFilePath,"a")
        changedClassHourDict = {}

        if os.path.isfile(hourlyChangedClass):
            changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)
        else:
            print ("Here hour dict")
            changedClassHourDict.clear();

        addedClassHourDict = {}
        if os.path.isfile(addedlogFilePath):
            addedClassHourDict = instatitechangedClassHourDict(addedlogFilePath)
        else:
            print ("Here added dict")
            addedClassHourDict.clear();

        deletedClassHourDict = {}
        if os.path.isfile(deletedlogFilePath):
            deletedClassHourDict = instatitechangedClassHourDict(deletedlogFilePath)
        else:
            print ("Here deleted dict")
            deletedClassHourDict.clear();

        file = Graph()
        file.parse(filePath, format="nt")
        for sub,pre,obj in file:
            allPreviousFilesAndCurrentFile.add((sub, pre, obj))

        sparqlEndpoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
        for s,p,o in file:
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

            checkWithInCurrentAndPreviousFilesQuery = """
            SELECT DISTINCT ?type
            WHERE {
             <""" + s.strip() + """> rdf:type ?type.
            }
            """
            resultFromCurrentAndPreviousFiles = allPreviousFilesAndCurrentFile.query(checkWithInCurrentAndPreviousFilesQuery)

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
                    if len(resultFromCurrentAndPreviousFiles.bindings) == 0:
                        if key in changedClassHourDict:
                            changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + 1
                        else:
                            changedClassHourDict[key] = 1
                    else:
                        for key2 in resultFromCurrentAndPreviousFiles:
                            if key2 in changedClassHourDict:
                                changedClassHourDict[key2] = int(str(changedClassHourDict[key2]).strip()) + 1
                            else:
                                changedClassHourDict[key2] = 1
                else:
                    if key in changedClassHourDict:
                        changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + 1
                    else:
                        changedClassHourDict[key] = 1
            #updating added dict
            if "added" in filePath:
                for result in results["results"]["bindings"]:
                    key = "<" + result["extractedType"]["value"] + ">"
                    if key == '''<http://exp.er.unknown>''':
                        if len(resultFromCurrentAndPreviousFiles.bindings) == 0:
                            if key in addedClassHourDict:
                                addedClassHourDict[key] = int(str(addedClassHourDict[key]).strip()) + 1
                            else:
                                addedClassHourDict[key] = 1
                        else:
                            for key2 in resultFromCurrentAndPreviousFiles:
                                if key2 in addedClassHourDict:
                                    addedClassHourDict[key2] = int(str(addedClassHourDict[key2]).strip()) + 1
                                else:
                                    addedClassHourDict[key2] = 1
                    else:
                        if key in addedClassHourDict:
                            addedClassHourDict[key] = int(str(addedClassHourDict[key]).strip()) + 1
                        else:
                            addedClassHourDict[key] = 1
            else:
                #updating deleted dict
                for result in results["results"]["bindings"]:
                    key = "<" + result["extractedType"]["value"] + ">"
                    if key == '''<http://exp.er.unknown>''':
                        if len(resultFromCurrentAndPreviousFiles.bindings) == 0:
                            if key in deletedClassHourDict:
                                deletedClassHourDict[key] = int(str(deletedClassHourDict[key]).strip()) + 1
                            else:
                                deletedClassHourDict[key] = 1
                        else:
                            for key2 in resultFromCurrentAndPreviousFiles:
                                if key2 in deletedClassHourDict:
                                    deletedClassHourDict[key2] = int(str(deletedClassHourDict[key2]).strip()) + 1
                                else:
                                    deletedClassHourDict[key2] = 1
                    else:
                        if key in deletedClassHourDict:
                            deletedClassHourDict[key] = int(str(deletedClassHourDict[key]).strip()) + 1
                        else:
                            deletedClassHourDict[key] = 1

        filePathMoved = filePath.replace('DBpediaChangeSet', 'DBpediaChangeSetDone')

        changedClassTS = open(hourlyChangedClass,"w+")
        addedClassTS = open(addedlogFilePath,"w+")
        deletedClassTS = open(deletedlogFilePath,"w+")

        for eachKeyHr in changedClassHourDict.keys():
            changedClassTS.write((eachKeyHr + " <http://er/c> " + str(changedClassHourDict[eachKeyHr]) + " . \n"))
        changedClassTS.close()

        for eachKeyAdd in addedClassHourDict.keys():
            addedClassTS.write((eachKeyAdd + " <http://er/c> " + str(addedClassHourDict[eachKeyAdd]) + " . \n"))
        addedClassTS.close()

        for eachKeyDel in deletedClassHourDict.keys():
            deletedClassTS.write((eachKeyDel + " <http://er/c> " + str(deletedClassHourDict[eachKeyDel]) + " . \n"))
        deletedClassTS.close()
        retryFile.close()

        # Move a file by renaming it's path
        os.rename(filePath, filePathMoved)
        time.sleep(1)
    lock.release()
    print ("lock released on directory " + path)
