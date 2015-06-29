import csv
targetDict = {}
MAXFILES = 17770  # 17770 is max in dataset
PRINTREADEVERY = 100
NUMBER_OF_FILES = 1000

directory = '../netflixdata/training_set/'

print('reading...')


def write_ratings(targetDict):
	print('writing...')
	for key in targetDict:
		filename = key % NUMBER_OF_FILES
		with open("%sby_user/%i.csv" % (directory, filename), "a") as csvfile:
			writer = csv.writer(csvfile)
			ratings = targetDict[key]
			for r in ratings:
				writer.writerow(r)
	print("written")


for currentFileIndex in range(1, MAXFILES):
	filename = 'mv_' + '{0:07d}'.format(currentFileIndex) + '.txt'
	with open(directory + filename,'r') as mFile:
		movid = 'TOBEFOUND'
		for line in mFile:
			if(':' in line): movid = int(line.split(':')[0])
			else:
				userid = int(line.split(',')[0])
				rating = int(line.split(',')[1])
				ratingTuple = (userid,movid,rating)
				if not userid in targetDict:
					targetDict[userid] = [ratingTuple]
				else:
					targetDict[userid].append(ratingTuple)

	if(currentFileIndex % PRINTREADEVERY == 0):
		percentageString = '{0:03f}'.format(currentFileIndex/float(MAXFILES)*100)
		print(str(currentFileIndex) + ' done (' + percentageString + ' %)')
		write_ratings(targetDict)
		targetDict.clear()




print('finished')