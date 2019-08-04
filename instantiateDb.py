import os
from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
import csv
from rdflib import Graph
import time
import re

Wrapper._returnFormatSetting = ['format']
sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")
sparqlEndpoint.setCredentials("admin", "admin")
sparqlEndpoint.setMethod(POST)
sparqlEndpoint.setReturnFormat(JSON)

#python 2.7 compatible code
def get_immediate_subdirectories(dirPath):
    allFilePath = []
    count = 1;
    for subdir, dirs, files in os.walk(dirPath):
        if len(files) > 0:
            for eachFile in files:
                if ".nt" in eachFile and not "retry" in eachFile:
                    allFilePath.append(subdir + "/" + eachFile)
    return allFilePath


def executeQuery (eachFile, file):
    queryToDelete = """
    delete data
    {
        graph <http://dbpedia/2015>
        {
    """ + file +""" }
    }
    """
    queryToInsert = """
    insert data
    {
        graph <http://dbpedia/2015>
        {
    """ + file +""" }
    }
    """
    sparqlEndpoint.setQuery(queryToDelete)
    mostGranularClasses = sparqlEndpoint.query().convert()

    sparqlEndpoint.setQuery(queryToInsert)
    mostGranularClasses = sparqlEndpoint.query().convert()


path = "/Users/anuj/PhD/DBpediaStartingPoint/2015/splitted"

# test_uri_1 = "<http:/abced>"
# test_uri_2 = "<https:/abced>"
# print (re.sub(r"(<http(s)?):/(/)?", r"\1://", test_uri_2))

allFiles = get_immediate_subdirectories(path)
for eachFile in allFiles:
    print(eachFile)
    file = open(eachFile,"r")
    uniqueTriples =  {}
    for eachLine in file:
        key = re.sub(r"(<http(s)?):/(/)?", r"\1://", eachLine)

        uniqueTriples[key] = 1
    file.close()

    uniqueTriplesFile = ""
    for eachKey in uniqueTriples:
        uniqueTriplesFile = uniqueTriplesFile + eachKey

    executeQuery (eachFile, uniqueTriplesFile)
    eachFileMoved = eachFile.replace("/splitted/","/uploaded/")
    os.rename(eachFile, eachFileMoved)
