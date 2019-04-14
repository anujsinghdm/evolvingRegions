import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET
DBsparqlEndPoint = SPARQLWrapper("https://dbpedia.org/sparql")
resultCount = 10000
counter = 1
setOfConcepts = set()
#while resultCount % 10000 == 0:
queryTogetEachClass = """select distinct ?concept where {[] a ?concept} LIMIT 10000 OFFSET """ +  str(counter * 10000)
DBsparqlEndPoint.setQuery(queryTogetEachClass)
DBsparqlEndPoint.setMethod(GET)
DBsparqlEndPoint.setReturnFormat(JSON)
results = DBsparqlEndPoint.query().convert()
resultCount = len(results["results"]["bindings"])
counter = counter + 1

conceptAndInstancesTable = []
for eachConcept in results["results"]["bindings"]:
    setOfConcepts.add(eachConcept["concept"]["value"])

for eachConceptinSet in setOfConcepts:
    queryToGetInstanceCount = """select count(?instance) as ?instanceCount
    where { ?instance ?predicate ?object .
            FILTER EXISTS {?instance rdf:type <""" + eachConceptinSet + """>}
    }
    """
    DBsparqlEndPoint.setQuery(queryToGetInstanceCount)
    DBsparqlEndPoint.setMethod(GET)
    DBsparqlEndPoint.setReturnFormat(JSON)
    totalCount = DBsparqlEndPoint.query().convert()["results"]["bindings"][0]["instanceCount"]["value"]
    print eachConceptinSet
    print totalCount
    #conceptAndInstancesTable[eachConceptinSet] =
