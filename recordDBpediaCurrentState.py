import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
import sys
from multiprocessing import Pool, TimeoutError
import glob, os
from importlib import reload
import datetime

currentStateLogFilePath = '../../DBpediaSnapshot/allConceptsWithTripleCounts.txt'
currentStateLogFO = open(currentStateLogFilePath,"a")

DBsparqlEndPoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
def instantiateAllConcepts(path):
    allConceptsDict = []
    allConceptsFO = open(path,"r")
    for eachConcept in allConceptsFO:
        allConceptsDict.append(eachConcept)
    return allConceptsDict

retryTable = []
def getCountOfTriples(conceptURI):
    print (conceptURI.strip())
    try:
        query = """
            select count(?subject) as ?countTriples
            where
            {
            ?subject rdf:type <""" + conceptURI.strip() + """>.
            ?subject ?predicate ?object.
            }
        """
        query = query.encode('UTF-8')
        DBsparqlEndPoint.setQuery(query)
        DBsparqlEndPoint.setTimeout("3600")
        DBsparqlEndPoint.setMethod(GET)
        DBsparqlEndPoint.setReturnFormat(JSON)
        results = DBsparqlEndPoint.query().convert()
        currentStateLogFO.write(conceptURI.strip() + "\n")
        currentStateFilePath = '../../DBpediaSnapshot/allConceptsWithTripleCounts/createdBy' + str(os.getpid()) + '.txt'
        currentStateFO = open(currentStateFilePath,"a")
        for count in results["results"]["bindings"]:
            currentStateFO.write(conceptURI.strip() + " ----- " + str(datetime.datetime.now()) + " ----- " + str(count["countTriples"]["value"]) + "\n")
    except Exception as e:
        print (e)
        currentStateLogFO.write(conceptURI.strip() + " ----- exception occured in " + str(os.getpid()) +"\n")
        if conceptURI in retryTable:
            if retryTable[conceptURI] < 4:
                retryTable[conceptURI] = retryTable[conceptURI] + 1
                getCountOfTriples(conceptURI)
        else:
            retryTable[conceptURI] = 1
            getCountOfTriples(conceptURI)

allConceptFilePath = '../../DBpediaSnapshot/allConcepts.txt'
if __name__ == '__main__':
    pool = Pool(processes=50)              # start 100 worker processes
    timingFilePath = ('../../DBpediaSnapshot/processTimings.txt')
    timingFileFOW = open(timingFilePath, 'w+' )
    timingFileFOW.write("Start time -- ")
    timingFileFOW.write(str(datetime.datetime.now()))

    queryTogetTotalTriplesInDBpedia = """select (count(?s) as ?count) where {?s ?p ?o}"""

    queryTogetTotalTriplesInDBpedia = queryTogetTotalTriplesInDBpedia.encode('UTF-8')
    DBsparqlEndPoint.setQuery(queryTogetTotalTriplesInDBpedia)
    DBsparqlEndPoint.setTimeout("36000")
    DBsparqlEndPoint.setMethod(GET)
    DBsparqlEndPoint.setReturnFormat(JSON)
    results = DBsparqlEndPoint.query().convert()
    timingFileFOW.write("\n" + str(results))
    timingFileFOW.close()

    pool.map(getCountOfTriples, instantiateAllConcepts(allConceptFilePath))

    resultsAgain = DBsparqlEndPoint.query().convert()

    timingFileFOA = open(timingFilePath, 'a' )
    timingFileFOA.write("\n" + str(resultsAgain))
    timingFileFOA.write("\nEnd time -- ")
    timingFileFOA.write(str(datetime.datetime.now()))
    timingFileFOA.close()
