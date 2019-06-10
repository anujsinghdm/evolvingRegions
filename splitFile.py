from fsplit.filesplit import FileSplit

fs = FileSplit(file='../../DBpediaStartingPoint/2015/dbpedia_2015_06_02.nt', splitsize=100000000, output_dir='../../DBpediaStartingPoint/2015/splitted/')
fs.split()
