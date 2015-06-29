import csv
targetDict = {}
MAXFILES = 17770  # 17770 is max in dataset
WRITE_TO_FILE_EVERY = 500
NUMBER_OF_FILES_OUT = 1000

directory = '../netflixdata/training_set/'


print('reading...')

def write_ratings(targetDict):
	print('writing...')
	keylist = list(targetDict.keys())
	lastFileName = ''
	csvfile = None
	fileopen = 0

	for key in sorted(keylist, key=lambda x: x % NUMBER_OF_FILES_OUT):
		filename = key % NUMBER_OF_FILES_OUT
		if(filename != lastFileName):
			lastFileName = filename
			if(csvfile is not None): csvfile.close()
			csvfile = open("%sby_user/%i.csv" % (directory, filename), "a", newline='')
			writer = csv.writer(csvfile)
			fileopen += 1
		ratings = targetDict[key]
		for r in ratings:
			writer.writerow(r)
	csvfile.close()
	print("written: " + str(fileopen))




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

	if(currentFileIndex % WRITE_TO_FILE_EVERY == 0):
		percentageString = '{0:03f}'.format(currentFileIndex/float(MAXFILES)*100)
		print(str(currentFileIndex) + ' done (' + percentageString + ' %)')
		write_ratings(targetDict)
		targetDict.clear()

write_ratings(targetDict)
print('finished')