from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib import Graph

queryFullDataset = """select distinct ?concept
where
{
	graph <http://dbpedia/2015>
    {
        [] a ?concept

    }
}
"""

queryChanges = """select distinct ?concept
where
{
	graph <http://changes>
    {
        [] a ?concept
    }
}
"""

allUniqueConcepts = set()
sparqlEndpoint = SPARQLWrapper("http://192.168.178.39:7200/repositories/Repo01")
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(JSON)


sparqlEndpoint.setQuery(queryFullDataset)

resultsFromDataset = sparqlEndpoint.query().convert()

for eachConceptFromDataset in resultsFromDataset["results"]["bindings"]:
    allUniqueConcepts.add(eachConceptFromDataset["concept"]["value"])

sparqlEndpoint.setQuery(queryChanges)
sparqlEndpoint.setMethod(GET)
sparqlEndpoint.setReturnFormat(JSON)
resultsFromChanges= sparqlEndpoint.query().convert()

for eachConceptfromnChanges in resultsFromChanges["results"]["bindings"]:
    allUniqueConcepts.add(eachConceptfromnChanges["concept"]["value"])

csvFo = open("./allChangesCsv.csv","w+")

allChangesGraph = Graph()
allChangesGraph.parse(data=open("/Users/anuj/PhD/ConsolidateChanges/allChanges.nt","r").read(), format="nt")
firstRow = "Concept"

for eachCol in range(1, 408):
    firstRow = firstRow + ","+ str(eachCol)

csvFo.write(firstRow + "\n")
for eachConcept in allUniqueConcepts:
    print (eachConcept)
    eachRow = eachConcept
    for eachCol in range(1, 408):
        queryToGetFrequency = """
        select ?changeFrequency
        where
        {

                <""" + str(eachConcept) + """> <http://hasChanges> ?changeInfo.
                ?changeInfo <http://change/consecutiveHour> \"""" + str(eachCol) + """\";
                            (<http://changeType/added> | <http://changeType/deleted>) ?changeFrequency

        }
        """
        try:
            frequency = allChangesGraph.query(queryToGetFrequency)
        except Exception as e:
            print ("retrying..." + str(e))
        sumAddDelFrequency = 0
        if len(frequency.bindings) == 0:
            eachRow = eachRow + ",0"
        else:
            for eachFrequency in frequency.bindings:
                sumAddDelFrequency = sumAddDelFrequency + int(eachFrequency["changeFrequency"])
            eachRow = eachRow + "," + str(sumAddDelFrequency)
    csvFo.write(eachRow + "\n")
