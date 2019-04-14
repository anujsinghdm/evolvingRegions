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

reload(sys)
sys.setdefaultencoding('utf8')

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
    print "lock acquired on directory " + path
    allPreviousFilesAndCurrentFile = Graph()
    for filePath in getAllFilePaths(path):
        print ("############################################################")
        print (filePath + str(os.getpid()))
        print ("############################################################")
        hourlyChangedClass = filePath.replace('DBpediaChangeSet', 'changedClasses')

        hourlyChangedClass = hourlyChangedClass.replace('/' + hourlyChangedClass.split('/')[len(filePath.split('/')) - 1], '.ttl')
        notFoundFilePath = hourlyChangedClass.replace('.ttl', 'notFound.ttl')
        retryFilePath = hourlyChangedClass.replace('.ttl', 'retry.ttl')
        logFilePath = hourlyChangedClass.replace('.ttl', 'log.ttl')

        addedlogFilePath = hourlyChangedClass.replace('.ttl', 'added.ttl')
        deletedlogFilePath = hourlyChangedClass.replace('.ttl', 'deleted.ttl')

        notFoundIRIFile = open(notFoundFilePath,"a")
        retryFile = open(retryFilePath,"a")
        logFile = open(logFilePath,"a")

        changedClassHourDict = {}
        if os.path.isfile(hourlyChangedClass):
            changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)

        if not os.path.isfile(hourlyChangedClass):
            changedClassHourDict.clear();

        addedClassHourDict = {}
        if os.path.isfile(addedlogFilePath):
            addedClassHourDict = instatitechangedClassHourDict(addedlogFilePath)

        if not os.path.isfile(addedlogFilePath):
            addedClassHourDict.clear();

        deletedClassHourDict = {}
        if os.path.isfile(deletedlogFilePath):
            deletedClassHourDict = instatitechangedClassHourDict(deletedlogFilePath)

        if not os.path.isfile(deletedlogFilePath):
            deletedClassHourDict.clear();

        file = Graph()
        file.parse(filePath, format="nt")
        sparqlEndpoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
        for sub,pre,obj in file:
            allPreviousFilesAndCurrentFile.add((sub, pre, obj))

        for s,p,o in file:
            logFile.write(str(s) + " process ID -- " + str(os.getpid()) + "\n")
            print s
            query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?extractedType
            WHERE {
            OPTIONAL {<""" + str(s.encode('utf-8')) + """> rdf:type ?type.}
            OPTIONAL{ <""" + str(s.encode('utf-8')) + """> <http://dbpedia.org/ontology/wikiPageRedirects> ?redirected.
            ?redirected rdf:type ?type1.}
            BIND(COALESCE(?type1, ?type) AS ?rdfType)
            BIND(COALESCE(?rdfType, "http://notFound") AS ?extractedType)
            }"""

            checkWithInCurrentAndPreviousFilesQuery = """
            SELECT DISTINCT ?type
            WHERE {
             <""" + str(s.encode('utf-8')) + """> rdf:type ?type.
            }
            """
            resultFromCurrentAndPreviousFiles = allPreviousFilesAndCurrentFile.query(checkWithInCurrentAndPreviousFilesQuery)

            try:
                query = query.encode('UTF-8')
                sparqlEndpoint.setQuery(query)
                sparqlEndpoint.setMethod(GET)
                sparqlEndpoint.setReturnFormat(JSON)
                results = sparqlEndpoint.query().convert()
            except Exception as e:
                print "retrying" + str(e)
                retryFile.write('<' + str(s.encode('utf-8')) + '>\n')
                time.sleep(1)
                continue
                #results = sparqlEndpoint.query().convert()

            for result in results["results"]["bindings"]:
                key = "<" + result["extractedType"]["value"] + ">"
                if key == '''<http://notFound>''':
                    if len(resultFromCurrentAndPreviousFiles.bindings) == 0:
                        if key in changedClassHourDict:
                            changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + 1
                        else:
                            changedClassHourDict[key] = 1
                    else:
                        for key2 in resultFromCurrentAndPreviousFiles:
                            print "******************************************************************"
                            print key2
                            print "******************************************************************"
                            if key2 in changedClassHourDict:
                                changedClassHourDict[key2] = int(str(changedClassHourDict[key2]).strip()) + 1
                            else:
                                changedClassHourDict[key2] = 1
                else:
                    if key in changedClassHourDict:
                        changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + 1
                    else:
                        changedClassHourDict[key] = 1

            if "added" in filePath:
                for result in results["results"]["bindings"]:
                    key = "<" + result["extractedType"]["value"] + ">"
                    if key == '''<http://notFound>''':
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
                for result in results["results"]["bindings"]:
                    key = "<" + result["extractedType"]["value"] + ">"
                    if key == '''<http://notFound>''':
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
        for eachKey in changedClassHourDict.keys():
            #copyTriple = (eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore')  str(changedClassHourDict[eachKey])+ )
            changedClassTS.write((eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore'))
            changedClassTS.write(str(changedClassHourDict[eachKey]))
            changedClassTS.write(" . \n")
        changedClassTS.close()
        print "%%%%%%%%%  changedClassTS object closed successfully"

        for eachKey in addedClassHourDict.keys():
            #copyTriple = (eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore')  str(changedClassHourDict[eachKey])+ )
            addedClassTS.write((eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore'))
            addedClassTS.write(str(addedClassHourDict[eachKey]))
            addedClassTS.write(" . \n")
        addedClassTS.close()
        print "%%%%%%%%%  addedlogFilePath object closed successfully"

        for eachKey in deletedClassHourDict.keys():
            #copyTriple = (eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore')  str(changedClassHourDict[eachKey])+ )
            deletedClassTS.write((eachKey + " <http://er/c> " ).encode('utf-8', errors='ignore'))
            deletedClassTS.write(str(deletedClassHourDict[eachKey]))
            deletedClassTS.write(" . \n")
        deletedClassTS.close()
        print "%%%%%%%%%  deletedlogFilePath object closed successfully"

        notFoundIRIFile.close()
        print "%%%%%%%%%  notFoundIRIFile object closed successfully"

        retryFile.close()
        print "%%%%%%%%%  retryFile object closed successfully"

        logFile.close()
        print "%%%%%%%%%  logFile object closed successfully"

        # Move a file by renaming it's path
        os.rename(filePath, filePathMoved)
        time.sleep(1)
    lock.release()
    print "lock released on directory " + path
