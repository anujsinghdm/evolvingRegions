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


def executeQuery (eachFile, fileChunk):

    queryInsert = """
    insert data
    {
        graph <http://changes>
        {
    """ + fileChunk +""" }
    }
    """

    # queryDelete = ("""
    # delete data
    # {
    #     graph <http://changes>
    #     {
    # """ + fileChunk +""" }
    # }
    # """).encode("UTF-8")

    sparqlEndpoint.setQuery(queryInsert)
    mostGranularClasses = sparqlEndpoint.query().convert()

    # if "added.nt" in eachFile:
    #     sparqlEndpoint.setQuery(queryDelete)
    #     mostGranularClasses = sparqlEndpoint.query().convert()
    #     sparqlEndpoint.setQuery(queryInsert)
    #     mostGranularClasses = sparqlEndpoint.query().convert()
    # else:
    #     sparqlEndpoint.setQuery(queryDelete)
    #     mostGranularClasses = sparqlEndpoint.query().convert()

path = "/Users/anuj/PhD/DBpediaChangeSet"

allFiles = sorted(get_immediate_subdirectories(path))
for eachFile in allFiles:
    print (eachFile)
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
    #eachFileMoved = eachFile.replace("/2015-07-triples/","/2015-07-triples-done/")
    # makeDir = eachFileMoved.replace(eachFileMoved.split("/")[len(eachFileMoved.split("/")) - 1], "")
    # if not os.path.exists(makeDir):
    #     os.makedirs(makeDir)
    # os.rename(eachFile, eachFileMoved)
