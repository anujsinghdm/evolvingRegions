from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib import Graph
Wrapper._returnFormatSetting = ['format']

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
sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")
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
dayTotalCountDict = {}
for eachCol in range(1, 35):
	if eachCol < 10:
		eachCol = "0" + str(eachCol)

	firstRow = (firstRow + ","+ str(eachCol))
	queryToGetTotalCountOfDay = """
		PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
		select (SUM(?intChangeFrequency) as ?sum)
		where
		{       ?changeInfo <http://change/day> \"""" + str(eachCol) + """\";
		                    (<http://changeType/added> | <http://changeType/deleted>) ?changeFrequency.
		         bind(xsd:integer(?changeFrequency) as ?intChangeFrequency)

		}
		"""
	totalDayCount = allChangesGraph.query(queryToGetTotalCountOfDay)
	dayTotalCountDict[str(eachCol)] = int(totalDayCount.bindings[0]["sum"])

print (dayTotalCountDict)

csvFo.write(firstRow + "\n")
for eachConcept in allUniqueConcepts:
	print (eachConcept)
	eachRow = eachConcept
	for eachCol in range(1, 35):
		if eachCol < 10:
			eachCol = "0" + str(eachCol)
		queryToGetFrequency = """
		select ?changeFrequency
		where
		{
		        <""" + str(eachConcept) + """> <http://hasChanges> ?changeInfo.
		        ?changeInfo <http://change/day> \"""" + str(eachCol) + """\";
		                    (<http://changeType/added> | <http://changeType/deleted>) ?changeFrequency

		}
	    """

		frequency = allChangesGraph.query(queryToGetFrequency)
		sumAddDelFrequency = 0
		if len(frequency.bindings) == 0:
			eachRow = eachRow + ",0"
		else:
			for eachFrequency in frequency.bindings:
				sumAddDelFrequency = sumAddDelFrequency + int(eachFrequency["changeFrequency"])
			changePercent = sumAddDelFrequency / int(dayTotalCountDict[str(eachCol)])
			eachRow = eachRow + "," + str(float("{0:.4f}".format(changePercent)))
			print (str(float("{0:.4f}".format(changePercent))))

	csvFo.write(eachRow + "\n")
