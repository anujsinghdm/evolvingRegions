import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
import sys
import math
import datetime


path = '../../DBpediaSnapshot/allConcepts.txt'
allConceptsFO = open(path,"w+")
DBsparqlEndPoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
DBsparqlEndPoint.setMethod(GET)
DBsparqlEndPoint.setReturnFormat(JSON)
DBsparqlEndPoint.setTimeout("36000")
queryTogetTotalTriplesInDBpedia = """select (count(?s) as ?NoOfTriplesInDBpedia) where {?s ?p ?o}"""
DBsparqlEndPoint.setQuery(queryTogetTotalTriplesInDBpedia)
allConceptsFO.write(str(datetime.datetime.now()))
allConceptsFO.write("\n Total triples in DBpedia-live ")
allTriples = DBsparqlEndPoint.query().convert()
allConceptsFO.write(str(allTriples["results"]["bindings"][0]["NoOfTriplesInDBpedia"]["value"]))
allConceptsFO.write("\n")
allConceptsFO.write(str(datetime.datetime.now()))
allConceptsFO.write("\n")
allConceptsFO.write("\n")

resultCount = 10000
counter = 0
setOfConcepts = set()

allConceptsFO.write("Recording of all concepts is starting...\n")
allConceptsFO.write(str(datetime.datetime.now()))
allConceptsFO.write("\n")
while resultCount % 10000 == 0:
    queryTogetEachClass = """select distinct ?concept where {[] a ?concept} LIMIT 10000 OFFSET """ +  str(counter * 10000)
    print (queryTogetEachClass)
    DBsparqlEndPoint.setQuery(queryTogetEachClass)
    results = DBsparqlEndPoint.query().convert()
    resultCount = len(results["results"]["bindings"])
    counter = counter + 1
    for eachConcept in results["results"]["bindings"]:
        allConceptsFO.write(eachConcept["concept"]["value"])
        allConceptsFO.write("\n")
allConceptsFO.write("Recording of all concepts is finished.\n")
allConceptsFO.write(str(datetime.datetime.now()))
