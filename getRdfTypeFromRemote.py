import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef

path = '../../DBpediaChangeSet'

def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

changedClassTS = open("../../changedClasses/changedClasses.ttl","w+")
for filePath in getAllFilePaths(path):
    print filePath
    file = Graph()
    file.parse(filePath, format="nt")
    sparqlEndpoint = SPARQLWrapper("http://live.dbpedia.org/sparql")
    for s,p,o in file:
        time.sleep(.5)
        print '#########' + str(s) + '#########'
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT DISTINCT ?extractedType
        WHERE {
        OPTIONAL {<""" + str(s) + """> rdf:type ?type.}
        OPTIONAL{ <""" + str(s) + """> <http://dbpedia.org/ontology/wikiPageRedirects> ?redirected.
        ?redirected rdf:type ?type1.}
        BIND(COALESCE(?type1, ?type) AS ?rdfType)
        BIND(COALESCE(?rdfType, "http://No/ClassFound") AS ?extractedType)
        }"""
        sparqlEndpoint.setQuery(query)
        sparqlEndpoint.setMethod(POST)
        sparqlEndpoint.setReturnFormat(JSON)
        results = sparqlEndpoint.query().convert()
        for result in results["results"]["bindings"]:
            changedClassTS.write( "<" + result["extractedType"]["value"] + "> <http://er/c> <" + str(s) + "> . \n" )
