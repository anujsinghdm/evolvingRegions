import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON

path = '../../DBpediaChangeSet'

def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

filePath =  getAllFilePaths(path)[1]
print filePath
file = Graph()
file.parse(filePath, format="nt")
index = 1
sparqlEndpoint = SPARQLWrapper("http://live.dbpedia.org/sparql")
for s,p,o in file:

    print '#########' + str(s) + '#########'
    time.sleep(.5)
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?extractedType
    WHERE {
    OPTIONAL {<""" + str(s) + """> rdf:type ?type.}
    OPTIONAL{ <""" + str(s) + """> <http://dbpedia.org/ontology/wikiPageRedirects> ?redirected.
    ?redirected rdf:type ?type1.}
    BIND(IF(BOUND(?type), ?type, ?type1) AS ?rdfType)
    BIND(IF(BOUND(?rdfType), ?rdfType, "No Class Found") AS ?extractedType)
    } """
    sparqlEndpoint.setQuery(query)
    sparqlEndpoint.setReturnFormat(JSON)
    results = sparqlEndpoint.query().convert()
    for result in results["results"]["bindings"]:
        print(result["extractedType"]["value"])
