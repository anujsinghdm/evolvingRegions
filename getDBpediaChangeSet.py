import urllib2, urllib, gzip, shutil, os
from bs4 import BeautifulSoup

def getAllAnchors( parentUrl ):
    html = urllib2.urlopen(parentUrl)
    htmlParser = BeautifulSoup(html, 'html.parser').find_all('a')
    return htmlParser

#python 2.7 compatible code
url = 'http://live.dbpedia.org/changesets/2015/06/'
#making day URL
for i, dayAnchor in enumerate(getAllAnchors(url)):
    dayUrl = url + dayAnchor.get('href')
    day = dayUrl.replace(url, '')
    #making hour URL
    for j, hrAnchor in enumerate(getAllAnchors(dayUrl)):
        if not j == 0 and i >= 12:
            hrUrl = dayUrl + hrAnchor.get('href')
            #making file URL
            for k, fileAnchor in enumerate(getAllAnchors(hrUrl)):
                fileName = fileAnchor.get('href')
                if not k == 0  and 'clear' not in fileName and 'reinserted' not in fileName:
                    fileUrl = hrUrl + fileName
                    hourlyFilename = ""
                    if "added.nt" in fileName:
                        hourlyFilename = hrUrl.split("/")[len(hrUrl.split("/")) - 2] + "added.nt"
                    if "removed.nt" in fileName:
                        hourlyFilename = hrUrl.split("/")[len(hrUrl.split("/")) - 2] + "deleted.nt"

                    if ".nt" in fileUrl:
                        downloadedFilePath = '../../DBpediaChangeSet/' + day + fileName
                        urllib.urlretrieve (fileUrl, downloadedFilePath)
                        with gzip.open(downloadedFilePath, 'rb') as f_in:
                            #extractedFilePath = downloadedFilePath.replace('.gz','')
                            extractedFilePath = '../../DBpediaChangeSet/' + day + "/" + hourlyFilename
                            with open(extractedFilePath, 'ab') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(downloadedFilePath)
                        print downloadedFilePath
