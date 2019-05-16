import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, N3
import sys
from multiprocessing import Pool, TimeoutError
from rdflib import Graph
import math
import datetime
import os


DBsparqlEndPoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
counter = 0
filename = 0
def getAllTriplesPerson(offset):
    defaultResults = 10000
    global counter
    global filename
    try:
        queryTogetEachClass = """construct{?s ?p ?o} where { graph <http://live.dbpedia.org>
          { ?s rdf:type dbo:Person. ?s ?p ?o.}
          } limit 10000 offset """ + str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setTimeout("36000")
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setReturnFormat(N3)
        results = DBsparqlEndPoint.query().convert()
        if counter % 50 == 0:
            filename = counter
        file = open("../../DBpediaSnapshot/6-may/Person/" + str(os.getpid()) + "-" + str(filename) + ".nt","ab")
        file.write(results)
        print (str(os.getpid()) + "--" + str(counter))
        counter = counter + 1
    except Exception as e:
        print (e)

def getAllTriplesPlace(offset):
    defaultResults = 10000
    global counter
    global filename
    try:
        queryTogetEachClass = """construct{?s ?p ?o} where {?s rdf:type dbo:Place. ?s ?p ?o.} limit 10000 offset """ + str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setTimeout("36000")
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setReturnFormat(N3)
        results = DBsparqlEndPoint.query().convert()
        if counter % 50 == 0:
            filename = counter
        file = open("../../DBpediaSnapshot/6-may/Place/" + str(os.getpid()) + "-" + str(filename) + ".nt","ab")
        file.write(results)
        print (str(os.getpid()) + "--" + str(counter))
        counter = counter + 1
    except Exception as e:
        print (e)

def getAllTriplesOrganisation(offset):
    defaultResults = 10000
    global counter
    global filename
    try:
        queryTogetEachClass = """construct{?s ?p ?o} where {?s rdf:type dbo:Organisation. ?s ?p ?o.} limit 10000 offset """ + str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setTimeout("36000")
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setReturnFormat(N3)
        results = DBsparqlEndPoint.query().convert()
        if counter % 50 == 0:
            filename = counter
        file = open("../../DBpediaSnapshot/6-may/Organisation/" + str(os.getpid()) + "-" + str(filename) + ".nt","ab")
        file.write(results)
        print (str(os.getpid()) + "--" + str(counter))
        counter = counter + 1
    except Exception as e:
        print (e)

def getAllTriplesWork(offset):
    defaultResults = 10000
    global counter
    global filename
    try:
        queryTogetEachClass = """construct{?s ?p ?o} where {?s rdf:type dbo:Work. ?s ?p ?o.} limit 10000 offset """ + str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setTimeout("36000")
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setReturnFormat(N3)
        results = DBsparqlEndPoint.query().convert()
        if counter % 50 == 0:
            filename = counter
        file = open("../../DBpediaSnapshot/6-may/Work/" + str(os.getpid()) + "-" + str(filename) + ".nt","ab")
        file.write(results)
        print (str(os.getpid()) + "--" + str(counter))
        counter = counter + 1
    except Exception as e:
        print (e)

def getAllTriplesSpecies(offset):
    defaultResults = 10000
    global counter
    global filename
    try:
        queryTogetEachClass = """construct{?s ?p ?o} where {
        graph <http://live.dbpedia.org>
        {?s rdf:type dbo:Species. ?s ?p ?o.}
        }
        limit 10000 offset """ + str(offset * 10000)
        print (queryTogetEachClass)
        DBsparqlEndPoint.setTimeout("36000")
        DBsparqlEndPoint.setQuery(queryTogetEachClass)
        DBsparqlEndPoint.setReturnFormat(N3)
        results = DBsparqlEndPoint.query().convert()
        if counter % 50 == 0:
            filename = counter
        file = open("../../DBpediaSnapshot/6-may/Species/" + str(os.getpid()) + "-" + str(filename) + ".nt","ab")
        file.write(results)
        print (str(os.getpid()) + "--" + str(counter))
        counter = counter + 1
    except Exception as e:
        print (e)

if __name__ == '__main__':
    timing = open("../../DBpediaSnapshot/6-may/recordTimestamp.txt", "a+")
    pool = Pool(processes=1)
    timing.write("Start downloading Work Data... " + str(datetime.datetime.now()))
    pool.map(getAllTriplesWork, range(5000))
    timing.write("Finish downloading Work Data... " + str(datetime.datetime.now()))
    timing.write("Start downloading Place Data... " + str(datetime.datetime.now()))
    pool.map(getAllTriplesPlace, range(5000))
    timing.write("Finish downloading Place Data... " + str(datetime.datetime.now()))
    timing.write("Start downloading Organisation Data... " + str(datetime.datetime.now()))
    pool.map(getAllTriplesOrganisation, range(5000))
    timing.write("Finish downloading Organisation Data... " + str(datetime.datetime.now()))
    timing.write("Start downloading Person Data... " + str(datetime.datetime.now()))
    pool.map(getAllTriplesPerson, range(5000))
    timing.write("Finish downloading Person Data... " + str(datetime.datetime.now()))
    timing.write("Start downloading Species Data... " + str(datetime.datetime.now()))
    pool.map(getAllTriplesSpecies, range(5000))
    timing.write("Finish downloading Species Data... " + str(datetime.datetime.now()))
