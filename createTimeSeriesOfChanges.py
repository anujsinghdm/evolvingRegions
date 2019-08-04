import os

#python 2.7 compatible code
def get_immediate_subdirectories(dirPath):
    allFilePath = []
    count = 1;
    for subdir, dirs, files in os.walk(dirPath):
        if len(files) > 0:
            for eachFile in files:
                if ".nt" in eachFile and not "retry" in eachFile:
                    allFilePath.append(subdir + "/" + eachFile)
    return allFilePath



path = "/Users/anuj/PhD/changedClasses"

allFiles = get_immediate_subdirectories(path)

consolidateChangesFilePath = "/Users/anuj/PhD/ConsolidateChanges/allChanges.nt"
fwo = open(consolidateChangesFilePath, "w+")
allConcepts = set ()
insatnceCount = 0
for eachFile in allFiles:
    filename = eachFile.split("/")[len(eachFile.split("/")) - 1].replace(".nt","").strip()
    typeOfChanges = ""
    hour = ""
    day = eachFile.split("/")[len(eachFile.split("/")) - 2].strip()
    if "added" in filename:
        typeOfChanges = "<http://changeType/added>"
        hour = int(filename.replace("added","")) + 1
    else:
        typeOfChanges = "<http://changeType/deleted>"
        hour = int(filename.replace("deleted","")) + 1

    print ("=======================")
    print (day)
    print (hour)
    print ((int(day) - 1) * 24 + int(hour))
    print ("=======================")
    fo = open(eachFile,"r")
    for eachChange in fo:
        concept = eachChange.split("""<http://er/c>""")[0].strip()
        allConcepts.add(concept)
        changeFrequency = eachChange.split("""<http://er/c>""")[1].split(".")[0].strip()
        triples = concept + """  <http://hasChanges> <http://change/""" + str(insatnceCount) + "> ."
        #triples = triples + """\n<http://change/""" + str(insatnceCount) + "> <http://change/day> \"" + str(day) + "\" ."
        triples = triples + """\n<http://change/""" + str(insatnceCount) + "> <http://change/hour> \"" + str(hour) + "\" ."
        triples = triples + """\n<http://change/""" + str(insatnceCount) + "> <http://change/consecutiveHour> \"" + str((int(day) - 1) * 24 + int(hour) ) + "\" ."
        triples = triples + """\n<http://change/""" + str(insatnceCount) + "> " + typeOfChanges + " \"" + str(changeFrequency) + "\" ."
        fwo.write(triples + "\n\n")
        insatnceCount = insatnceCount + 1

for eachConcept in allConcepts:
    fwo.write(eachConcept + """ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .\n""" )
