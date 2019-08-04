from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
import csv

Wrapper._returnFormatSetting = ['format']
sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")
sparqlEndpoint.setCredentials("admin", "admin")
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(JSON)
wo = open("./initialCounts.csv","w+")


def calCulateCounts(concept):
    offset = 0
    result = 1
    total = 0
    while int(result) != 0:
        query = ("""
        prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        prefix owl: <http://www.w3.org/2002/07/owl#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        select (count(*) as ?count)
        where
        {
          ?s ?p ?o
          {
            select ?s
            where {
            graph <http://dbpedia/2015>
                  {
                     ?s rdf:type <""" + concept + """>
                  }
            }
            limit  10000
            offset """ + str(offset * 10000) + """
          }
        }
        """).encode('utf-8')
        sparqlEndpoint.setQuery(query)
        result = sparqlEndpoint.query().convert()["results"]["bindings"][0]["count"]["value"]
        total = total + int(result)
        print (concept + "    " + result + "    " + str(total))
        offset = offset + 1
    wo.write(concept + "," + str(total))



fo = open("./allChangesCsv.csv", "r")
file = csv.reader(fo, delimiter=',')
bool = True
for eachRow in file:
    if bool == True:
        bool = False
    else:
        calCulateCounts(eachRow[0])
