from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
import csv

Wrapper._returnFormatSetting = ['format']
sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")
sparqlEndpoint.setCredentials("admin", "admin")
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(JSON)
csvO = open("./initialCounts.csv","r")
initialCountsCsv = csv.reader(csvO, delimiter=',')

allChangesCsvOld = open("./allChangesCsv.csv","r")
allChangesCsvNew = open("./allChangesCsvNew.csv","w+")
#initialCountsCsv = csv.reader(csvO, delimiter=',')

csvDict = {}

for eachRow in initialCountsCsv:
    csvDict[eachRow[0]] = eachRow[1]

getMostGranularClassesQuery = """
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

select ?class
where
{
  graph <dbpedia/2015/tbox>
        {
         ?class rdf:type owl:Class.
          FILTER NOT EXISTS {?anyOtherClass rdfs:subClassOf ?class}
        }
}
"""

sparqlEndpoint.setQuery(getMostGranularClassesQuery)
mostGranularClasses = sparqlEndpoint.query().convert()

for eachRowInAllChangesCsvOld in allChangesCsvOld:
    if eachRowInAllChangesCsvOld.split(",")[0] in csvDict:
        triplesForConcept = csvDict[eachRowInAllChangesCsvOld.split(",")[0]]
        allChangesCsvNew.write(triplesForConcept + ",")
        allChangesCsvNew.write(eachRowInAllChangesCsvOld + "\n")
    else:
        allChangesCsvNew.write("Total triples," + eachRowInAllChangesCsvOld.split(",")[0] + "\n")



totalTriples = 0
for eachClass in mostGranularClasses["results"]["bindings"]:
    if eachClass["class"]["value"] in csvDict:
        totalTriples = totalTriples + int(csvDict[eachClass["class"]["value"]])
    else:
        ()
        # print ("-----------")
        # print (eachClass)
        # print ("-----------")

#print (totalTriples)
