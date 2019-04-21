import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
import sys
from multiprocessing import Pool, TimeoutError
from rdflib import Graph
import math
import datetime

DBsparqlEndPoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")

def getAllTriples(offset):
    try:
        counter = 1
        resultAccum = ""
        queryTogetEachClass = """select ?s ?p ?o where {?s ?p ?o} LIMIT 10000 OFFSET """ +  str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setMethod(GET)
        DBsparqlEndPoint.setReturnFormat(JSON)
        results = DBsparqlEndPoint.query().convert()
        filePath = ('../../DBpediaSnapshot/allFilesWithTriples/offset' + str(offset) +'.json')

        FO = open(filePath, 'w' )
        FO.write(str(results))
    except Exception as e:
        print (e)

if __name__ == '__main__':
    pool = Pool(processes=90)              # start 100 worker processes
    timingFilePath = ('../../DBpediaSnapshot/processTimings.txt')
    timingFileFOW = open(timingFilePath, 'w+' )
    timingFileFOW.write("Start time -- ")
    timingFileFOW.write(str(datetime.datetime.now()))
    timingFileFOW.close()
    pool.map(getAllTriples, range(63000))
    timingFileFOA = open(timingFilePath, 'a' )
    timingFileFOA.write("\nEnd time -- ")
    timingFileFOA.write(str(datetime.datetime.now()))
    timingFileFOA.close()
