import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
import sys


DBsparqlEndPoint = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
resultCount = 10000
counter = 0
setOfConcepts = set()
path = '../../DBpediaSnapshot/allConcepts.txt'
# allConceptsFO = open(path,"w+")
# while resultCount % 10000 == 0:
#     queryTogetEachClass = """select distinct ?concept where {[] a ?concept} LIMIT 10000 OFFSET """ +  str(counter * 10000)
#     print (queryTogetEachClass)
#     DBsparqlEndPoint.setQuery(queryTogetEachClass)
#     DBsparqlEndPoint.setMethod(GET)
#     DBsparqlEndPoint.setReturnFormat(JSON)
#     results = DBsparqlEndPoint.query().convert()
#     resultCount = len(results["results"]["bindings"])
#     counter = counter + 1
#     for eachConcept in results["results"]["bindings"]:
#         allConceptsFO.write(eachConcept["concept"]["value"])
#         allConceptsFO.write("\n")

queryTogetTotalTriplesInDBpedia = """select count(?s) as ?count
where
{
  ?s ?p ?o

  minus

  {?s rdf:type ?anything}

}"""

queryTogetTotalTriplesInDBpedia = queryTogetTotalTriplesInDBpedia.encode('UTF-8')
DBsparqlEndPoint.setQuery(queryTogetTotalTriplesInDBpedia)
DBsparqlEndPoint.setTimeout("36000")
DBsparqlEndPoint.setMethod(GET)
DBsparqlEndPoint.setReturnFormat(JSON)
results = DBsparqlEndPoint.query().convert()
print (results)
