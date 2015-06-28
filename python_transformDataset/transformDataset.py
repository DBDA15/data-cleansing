targetDict = {}
MAXFILES = 17770 # 17770 is max in dataset
PRINTREADEVERY = 100
PRINTWRITEEVERY = 100000

dir = 'D:/Eigene Dateien/HPI/DBDA/prize_dataset/download/training_set/training_set/'

print('reading...')
currentFileIndex = 1
while currentFileIndex <= MAXFILES:
	filename = 'mv_' + '{0:07d}'.format(currentFileIndex) + '.txt'
	mFile = open(dir + filename ,'r')
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
	mFile.close()
	if(currentFileIndex % PRINTREADEVERY == 0): 
		percentageString = '{0:03f}'.format(currentFileIndex/MAXFILES*100)
		print(str(currentFileIndex) + ' done (' + percentageString + ' %)')
	currentFileIndex += 1

print('writing...')
fileout = open('_output.txt','w')
writeCount = 0
numberOfUsers = len(targetDict)

for key in targetDict:
	fileout.write(str(targetDict[key]) + '\n')
	writeCount += 1
	
	if(writeCount % PRINTWRITEEVERY == 0): 
		percentageString = '{0:03f}'.format(writeCount/numberOfUsers*100)
		print(str(writeCount) + ' done (' + percentageString + ' %)')
fileout.close()

print('finished')