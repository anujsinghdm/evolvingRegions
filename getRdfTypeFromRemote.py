import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle

path = '../../DBpediaChangeSet/01/00'

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
    changedClassLog = open(hourlyChangedClass,"r")
    for eachLine in changedClassLog:
        eachLine = eachLine.decode('utf-8').strip()
        intialDictionary[eachLine.split('<http://er/c>')[0].strip()] = eachLine.split('<http://er/c>')[1].replace('.','').strip()
    return intialDictionary

for filePath in getAllFilePaths(path):
    print "############################################################"
    print filePath
    print "############################################################"
    hourlyChangedClass = filePath.replace('DBpediaChangeSet', 'changedClasses')
    hourlyChangedClass = hourlyChangedClass.replace('/' + hourlyChangedClass.split('/')[len(filePath.split('/')) - 1], '.ttl')
    changedClassHourDict = {}
    if os.path.isfile(hourlyChangedClass):
        changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)

    if not os.path.isfile(hourlyChangedClass):
        changedClassHourDict.clear();
    file = Graph()
    file.parse(filePath, format="nt")
    sparqlEndpoint = SPARQLWrapper("http://live.dbpedia.org/sparql")
    for s,p,o in file:
        time.sleep(.5)
        print s.encode('utf-8', errors='ignore')
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?extractedType
        WHERE {
        OPTIONAL {<""" + s.encode('utf-8', errors='ignore') + """> rdf:type ?type.}
        OPTIONAL{ <""" + s.encode('utf-8', errors='ignore') + """> <http://dbpedia.org/ontology/wikiPageRedirects> ?redirected.
        ?redirected rdf:type ?type1.}
        BIND(COALESCE(?type1, ?type) AS ?rdfType)
        BIND(COALESCE(?rdfType, "http://www.w3.org/2002/07/owl#Thing") AS ?extractedType)
        }"""
        sparqlEndpoint.setQuery(query)
        sparqlEndpoint.setMethod(POST)
        sparqlEndpoint.setReturnFormat(JSON)
        results = sparqlEndpoint.query().convert()
        for result in results["results"]["bindings"]:
            key = "<" + result["extractedType"]["value"] + ">"
            if key in changedClassHourDict:
                changedClassHourDict[key] = int(changedClassHourDict[key]) + 1
            else:
                changedClassHourDict[key] = 1

    filePathMoved = filePath.replace('DBpediaChangeSet', 'DBpediaChangeSetDone')
    changedClassTS = open(hourlyChangedClass,"w+")
    for eachKey in changedClassHourDict.keys():
        copyTriple = (eachKey + " <http://er/c> "+ str(changedClassHourDict[eachKey])+ " . \n" ).encode('utf-8', errors='ignore')
        changedClassTS.write(copyTriple)
    changedClassTS.close()
    # Move a file by renaming it's path
    os.rename(filePath, filePathMoved)
    time.sleep(1)
