import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading

reload(sys)
sys.setdefaultencoding('utf8')

path = '../../DBpediaChangeSet/01/00'
lock = threading.Lock()


def getAllFilePaths(dirPath):
    allFilePath = []
    for subdir, dirs, files in os.walk(dirPath):
        for file in files:
            filePath = os.path.join(subdir, file)
            if filePath.endswith('.nt'):
                allFilePath.append(filePath)
    return allFilePath

def instatitechangedClassHourDict (filePath):
    intialDictionary = {}
    changedClassLog = open(filePath,"r")
    for eachLine in changedClassLog:
        eachLine = eachLine.strip()
        intialDictionary[eachLine.split('<http://er/c>')[0].strip()] = eachLine.split('<http://er/c>')[1].replace('.','').strip()
    return intialDictionary



filePath = '../../changedClasses/01/00notFound.ttl'
file = open(filePath,"r")
sparqlEndpointLive = SPARQLWrapper("http://dbpedia-live.openlinksw.com/sparql")
sparqlEndpoint = SPARQLWrapper("https://dbpedia.org/sparql")
count = 0
for s in file:
    print str(s)
    queryGetRDFTypeFromConcept = """
    SELECT distinct ?typeOfSimilarResource
    {
    {
    SELECT      distinct *
                WHERE {
                FILTER EXISTS {""" + str(s.encode('utf-8')) + """ dct:subject ?concept.}
                ?concept ^dct:subject ?similarResource.
                ?similarResource rdf:type ?typeOfSimilarResource.
                FILTER NOT EXISTS {?typeOfSimilarResource rdfs:subClassOf ?topLevelClass}
    }
    }
    UNION
    {
    SELECT      distinct *
                WHERE
                {
                FILTER EXISTS {""" + str(s.encode('utf-8')) + """ dct:subject ?concept.}
                ?concept ^dct:subject ?similarResource.
                ?similarResource rdf:type ?typeOfSimilarResource.
                ?typeOfSimilarResource rdfs:subClassOf owl:Thing.
    }
    }
    }
    """
    queryGetRdfTypeFromNormalDbpedia = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT DISTINCT ?extractedType
    WHERE {
        """ + str(s.encode('utf-8')) + """ rdf:type ?extractedType.
    }"""
    #Get RDF type from similar resources in Live DBpedia
    queryGetRDFTypeFromConcept = queryGetRDFTypeFromConcept.encode('UTF-8')
    sparqlEndpointLive.setQuery(queryGetRDFTypeFromConcept)
    sparqlEndpointLive.setMethod(GET)
    sparqlEndpointLive.setReturnFormat(JSON)
    resultsFromLive = sparqlEndpointLive.query().convert()
    if len(resultsFromLive["results"]["bindings"]) == 0:
        ()
    else:
        count = count + 1
    print count

    # queryGetRdfTypeFromNormalDbpedia = queryGetRdfTypeFromNormalDbpedia.encode('UTF-8')
    # sparqlEndpoint.setQuery(queryGetRdfTypeFromNormalDbpedia)
    # sparqlEndpoint.setMethod(GET)
    # sparqlEndpoint.setReturnFormat(JSON)
    # results = sparqlEndpoint.query().convert()
    # if len(results["results"]["bindings"]) == 0:
    #     ()
    # else:
    #     count = count + 1
    # print count
