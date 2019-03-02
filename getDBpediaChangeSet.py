
import urllib2, urllib, gzip, shutil, os
from bs4 import BeautifulSoup

def getAllAnchors( parentUrl ):
    html = urllib2.urlopen(parentUrl)
    htmlParser = BeautifulSoup(html, 'html.parser').find_all('a')
    return htmlParser


url = 'http://live.dbpedia.org/changesets/2019/03/'
#making day URL
for i, dayAnchor in enumerate(getAllAnchors(url)):
    if not i == 0:
        dayUrl = url + dayAnchor.get('href')

        #making hour URL
        for j, hrAnchor in enumerate(getAllAnchors(dayUrl)):
            if not j == 0:
                hrUrl = dayUrl + hrAnchor.get('href')

                #making file URL
                for k, fileAnchor in enumerate(getAllAnchors(hrUrl)):
                    fileName = fileAnchor.get('href')
                    if not k == 0  and 'clear' not in fileName and 'reinserted' not in fileName:
                        fileUrl = hrUrl + fileName
                        downloadedFilePath = '../DBpediaChangeSet/' + fileUrl.replace(url,'').replace('/','_').replace('.','_',1)
                        urllib.urlretrieve (fileUrl, downloadedFilePath)
                        with gzip.open(downloadedFilePath, 'rb') as f_in:
                            extractedFilePath = downloadedFilePath.replace('.gz','')
                            with open(extractedFilePath, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(downloadedFilePath)
                        print downloadedFilePath
