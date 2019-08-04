path = "/Users/anuj/PhD/DBpediaStartingPoint/dbpedia_2015_06_02.nt"
fo = open(path,"r")
uniqueTriples = {}
for eachLine in fo:
    uniqueTriples[eachLine] = 1

wfo = open("/Users/anuj/PhD/DBpediaStartingPoint/unique_triples_dbpedia_2015_06_02.nt", "w+")
for eachKey in uniqueTriples:
    print (eachKey)
    wfo.write(eachKey)
