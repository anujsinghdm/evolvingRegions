import glob, os
import pprint
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON

path = '../../DBpediaChangeSet'
sparqlEndpoint = SPARQLWrapper("http://live.dbpedia.org/sparql")

def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

filePath =  getAllFilePaths(path)[1]
file = Graph()
file.parse(filePath, format="nt")
for s,p,o in file:
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?type
    WHERE {
    <""" + str(s) + """> rdf:type ?type } """
    sparqlEndpoint.setQuery(query)
    sparqlEndpoint.setReturnFormat(JSON)
    results = sparqlEndpoint.query().convert()
    for result in results["results"]["bindings"]:
        print(result["type"]["value"])
#sparqlEndpoint.setQuery("""
#    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#    SELECT ?label
#    WHERE { <http://dbpedia.org/resource/Asturias> rdfs:label ?label }
#""")
#sparqlEndpoint.setReturnFormat(JSON)
#results = sparqlEndpoint.query().convert()

print results
