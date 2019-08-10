import glob, os
import pprint
import time
from rdflib import Graph
from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST, GET, TURTLE
from rdflib.plugin import register, Serializer, Parser
from rdflib import URIRef
import pickle
import sys
import threading
import invokeParallelProcess
import re
import gc
import json
Wrapper._returnFormatSetting = ['format']

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


def getClasses (path):
    lock.acquire()
    print ("lock acquired on directory " + path)
    hourlyChangedClass = "../../changedClasses/" + invokeParallelProcess.day + "/"+ path
    filePath = hourlyChangedClass.replace('changedClasses', 'DBpediaChangeSet')
    print ("############################################################")
    print (filePath)
    print ("############################################################")

    retryResources = []
    changedClassHourDict = {}
    changeLogDir = hourlyChangedClass.replace(hourlyChangedClass.split("/")[len(hourlyChangedClass.split("/")) - 1],"")
    if not os.path.exists(changeLogDir):
        os.makedirs(changeLogDir)
    print (changeLogDir)
    print (hourlyChangedClass)
    if os.path.isfile(hourlyChangedClass):
        changedClassHourDict = instatitechangedClassHourDict(hourlyChangedClass)
    else:
        changedClassHourDict.clear();

    tempFile = ""

    file = Graph()
    miniG = Graph()
    for eachTriple in open(filePath, "r"):
        try:
            miniG.parse(data=eachTriple,format="nt")
            tempFile = tempFile + eachTriple
        except Exception as e:
            retryResources.append(eachTriple)
            print (eachTriple)

    file.parse(data=tempFile, format="nt")
    print(len(file))
    unique_sub = {}
    sparqlEndpoint = SPARQLWrapper("http://localhost:8000/v1/graphs/sparql")

    for sub,pre,obj in file:
        if sub in unique_sub:
            unique_sub[sub] = unique_sub[sub] + 1
        else:
            unique_sub[sub] = 1

    print (len(unique_sub))
    # changeGraph = Graph()
    # changeGraph.parse(data=open("./allChanges.nt", "r").read(), format="ttl")
    # print ("Graph generated")

    counter = 0
    length = len(unique_sub)
    subjectURIs = ""
    projection = "select distinct "
    mapping_dict = {}
    for i, s in enumerate(unique_sub, start = 1):
        #print (s)
        counter = counter + 1
        tempVar = """?temp_type_""" + str(counter)
        var = """?type_""" + str(counter)
        if counter < 150:
            mapping_dict["""?type_""" + str(counter)] = s
            if  i == length:
                projection = projection + """ ?type_""" + str(counter)
                subjectURIs = subjectURIs + """{ optional {<""" + s.strip() + """> rdf:type """ + tempVar +""".}
                  bind(if (bound(""" + tempVar + """), """ + tempVar + """, "http://exp.er.unknown") as """ + var + """)} \n"""
            else:
                projection = projection + """ ?type_""" + str(counter)
                subjectURIs = subjectURIs + """{ optional {<""" + s.strip() + """> rdf:type """ + tempVar +""".}
                  bind(if (bound(""" + tempVar + """), """ + tempVar + """, "http://exp.er.unknown") as """ + var + """)} \n union \n"""

        # if counter == 200:
        #     projection = projection + """ ?type_""" + str(counter)
        #     mapping_dict["""?type_""" + str(counter)] = s
        #     subjectURIs = subjectURIs + """{ optional {<""" + s.strip() + """> rdf:type """ + tempVar +""".}
        #       bind(if (bound(""" + tempVar + """), """ + tempVar + """, "http://exp.er.unknown") as """ + var + """)}  \n"""

        if counter == 150 or i == length:
            print (i)
            if i == length:
                ()
            else:
                projection = projection + """ ?type_""" + str(counter)
                mapping_dict["""?type_""" + str(counter)] = s
                subjectURIs = subjectURIs + """{ optional {<""" + s.strip() + """> rdf:type """ + tempVar +""".}
                  bind(if (bound(""" + tempVar + """), """ + tempVar + """, "http://exp.er.unknown") as """ + var + """)}  \n"""


            query = ("""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            """ + projection + """
            WHERE {
            """ + subjectURIs +
            """}""")
            try:
                sparqlEndpoint.setMethod(GET)
                sparqlEndpoint.setReturnFormat(JSON)
                sparqlEndpoint.setQuery(query)
                results = sparqlEndpoint.query().convert()
            except Exception as e:
                print ("retrying...\n" + subjectURIs)
                retryResources.append(subjectURIs)
                time.sleep(1)
                continue

            counter = 0
            subjectURIs = ""
            projection = "select distinct "

            for eachType in results["results"]["bindings"]:
                #updating hour dict
                key = "<" + str(eachType[list(eachType.keys())[0]]["value"])+ ">"
                subjectCount = unique_sub[mapping_dict[("?" + list(eachType.keys())[0])]]
                if key in changedClassHourDict:
                    changedClassHourDict[key] = int(str(changedClassHourDict[key]).strip()) + subjectCount
                else:
                    changedClassHourDict[key] = subjectCount

    filePathMoved = filePath.replace('DBpediaChangeSet', 'DBpediaChangeSetDone')
    retryFilePath = hourlyChangedClass.replace('.nt', 'retry.nt')
    retryFile = open(retryFilePath,"w+")

    for eachToBeRetry in retryResources:
        retryFile.write(eachToBeRetry + "\n")

    changedClassTS = open(hourlyChangedClass,"w+")

    try:
        for eachKeyHr in changedClassHourDict.keys():
            changedClassTS.write((eachKeyHr + " <http://er/c> " + str(changedClassHourDict[eachKeyHr]) + " . \n"))
        changedClassTS.close()
        retryFile.close()
    except Exception as e:
        print (str(e))

    #Move a file by renaming it's path
    os.rename(filePath, filePathMoved)
    del file
    gc.collect()
    time.sleep(1)
    lock.release()
    print ("lock released on directory " + path)
