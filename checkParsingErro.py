from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET

graph = Graph()
print ("hello")
newFile = open("/Users/anuj/PhD/DBpediaStartingPoint/2015/splitted/mod_97.ttl","wb+")
fo = open("/Users/anuj/PhD/DBpediaStartingPoint/dbpedia_2015-04.nt", "r")
sparqlEndpoint = SPARQLWrapper("http://192.168.178.39:7200/repositories/Repo01/statements")
for eachTriple in fo:
    try:
        query = ('''INSERT DATA
        {
        graph <http://tbox>
        {'''
        + eachTriple +
        '''}
        }''').encode('utf-8')
        #query = """ select * where {?s ?p ?o} limit 1"""
        print (query)
        sparqlEndpoint.setQuery(query)
        sparqlEndpoint.setMethod(POST)
        #sparqlEndpoint.queryType = "INSERT"
        sparqlEndpoint.setReturnFormat(JSON)
        sparqlEndpoint.query()
    except Exception as e:
        newFile.write(eachTriple)
