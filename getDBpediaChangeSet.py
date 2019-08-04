import urllib2, urllib, gzip, shutil, os
from bs4 import BeautifulSoup

def getAllAnchors( parentUrl ):
    html = urllib2.urlopen(parentUrl)
    htmlParser = BeautifulSoup(html, 'html.parser').find_all('a')
    return htmlParser

#python 2.7 compatible code
url = 'http://downloads.dbpedia.org/live/changesets/2015/08/'
#making day URL
for i, dayAnchor in enumerate(getAllAnchors(url)):
    dayUrl = url + dayAnchor.get('href')
    day = dayUrl.replace(url, '')
    print (day)
    #making hour URL
    for j, hrAnchor in enumerate(getAllAnchors(dayUrl)):
        if not j == 0 and i != 0 and int(day.replace("/","")) > 30:
            hrUrl = dayUrl + hrAnchor.get('href')
            #making file URL
            for k, fileAnchor in enumerate(getAllAnchors(hrUrl)):
                fileName = fileAnchor.get('href')
                if not k == 0  and 'clear' not in fileName and 'reinserted' not in fileName:
                    fileUrl = hrUrl + fileName
                    hour = fileUrl.split("/")[len(fileUrl.split("/")) - 2]
                    if "added.nt" in fileName:
                        dailyFilename = hrUrl.split("/")[len(hrUrl.split("/")) - 3] + "added.nt"
                    if "removed.nt" in fileName:
                        dailyFilename = hrUrl.split("/")[len(hrUrl.split("/")) - 3] + "deleted.nt"
                    if ".nt" in fileUrl:
                        downloadedFilePath = '../../DBpediaChangeSet/' + day + fileName
                        if not os.path.exists('../../DBpediaChangeSet/' + day):
                            os.makedirs('../../DBpediaChangeSet/' + day)
                        urllib.urlretrieve (fileUrl, downloadedFilePath)

                        with gzip.open(downloadedFilePath, 'rb') as f_in:
                            extractedFilePath = ('../../DBpediaChangeSet/' + day + dailyFilename)
                            extractedFilePath = extractedFilePath.replace(".gz","")
                            with open(extractedFilePath, 'ab') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(downloadedFilePath)
                        print (fileUrl)
