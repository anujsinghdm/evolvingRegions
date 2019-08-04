import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE, N3
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess
import re
import gc

Wrapper._returnFormatSetting = ['format']

allChangesQuery = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
CONSTRUCT {
    ?s rdf:type ?type
}
WHERE
{
    graph <http://changes>
    {
       ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type.
    }
}
limit 1000
"""

#query within changes
sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")
sparqlEndpoint.setQuery(allChangesQuery)
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(TURTLE)
allChanges = sparqlEndpoint.query().convert()

changeGraph = Graph()
changeGraph.parse(data=allChanges, format="ttl")
fo = open("./allChanges.nt","wb")
fo.write(changeGraph.serialize(format='nt'))
