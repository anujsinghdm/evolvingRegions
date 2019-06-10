import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess
import re
import gc

allChangesQuery = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
CONSTRUCT {
    ?s rdf:type ?type
}
WHERE
{
    graph <http://changes>
    {
       ?s rdf:type ?type
    }
}
"""

#query within changes
sparqlEndpoint = SPARQLWrapper("http://192.168.178.39:7200/repositories/Repo01")
sparqlEndpoint.setQuery(allChangesQuery)
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(TURTLE)
allChanges = sparqlEndpoint.query().convert()

# changeGraph = Graph()
# changeGraph.parse(data=allChanges, format="nt")
fo = open("./allChanges.nt","wb")
fo.write(allChanges)
