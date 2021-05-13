from pymongo import MongoClient
import random
import time
#from config import config


from pprint import pprint

def clear_sides(line):
	i = 0
	line = list(line)
	while ((line[i] == ' ')|(line[i] == "'")|(line[i] == '"')|(line[i] == '\n')):
		line.pop(i)
	i = -1	
	while ((line[i] == ' ')|(line[i] == "'")|(line[i] == '"')|(line[i] == '\n')):
		line.pop(i)
	line = "".join(line)
	return line

def import_to_db(year, db, test_fall_chance = 0):
	count_lines = len(open('Odata' + year + 'File.csv').readlines(  ))
	print(count_lines)
	with open("log.txt") as log_file:
		log = log_file.readline()
		if (log == ""):
			line_num = 0
		else:
			line = log.split(";")
			if (int(year)<int(line[1])):
				return 1
			else:
				line_num = int(line[0])
	duration = float(time.time())
	with open('Odata' + year + 'File.csv') as csvfile:
		header = csvfile.readline()
		header = header.replace('\n','')
		header = header.replace('"','')
		header = header.split(";")
		header.append("year")

		n = 0
		while n < line_num:
			csvfile.readline()
		for line in csvfile:
			arg_lst = []
			line = line.split(";")
			OutID = (str(line[0].replace('"','')))
			arg_lst.append(OutID)
			for i in range(1, len(line)):
				line[i] = clear_sides(line[i])
				try:
					to_float = line[i].replace(",",".")
					line[i] = float(to_float)
				except:
					line[i] = line[i].replace("'","’")					
				arg_lst.append(line[i])
			arg_lst.append(year)
			
			participant_info = dict(zip(header, arg_lst))
			n+=1
			log_file = open("log.txt", "w")
			log_file.write(str(n) + ";" + year)
			log_file.close()
			
			

			try:
				result = db.participant_info.insert_one(participant_info)
			#except psycopg2.InterfaceError as e:
			'''except:
				log_file = open("log.txt", "w")
				log_file.write(str(n - 1) + ";" + year)
				log_file.close()
				#raise e'''
			if(n==1000):
			    break
			if (random.randint(0, test_fall_chance) == 1):
				print("Потеряно соединение с базой данных")
				db.close()
		duration = round((float(time.time()) - duration), 4)
		with open('upload_time.txt','a') as upload_time:
			upload_time.write('Data from Odata' + year + 'File.csv uploaded in ' + str(duration) + ' seconds\n')
	return 0	







try:
	open("log.txt").close()	
except FileNotFoundError:
	open("log.txt","w").close()	

years = ['2019', '2020']

test_fall = input("Желеете протестировать сценарий 'падения' базы данных? (y/n)")
if (test_fall == "y"):
	test_fall_chance = int(input("Пожалуйста введите n (вероятность падения базы данных после анализа строчки - 1/n): "))
else:
	test_fall_chance = 0
client = MongoClient("mongodb://localhost:27017")
db=client.ZNO_data


drop = input("Желеете удалить базу данных? (y/n)")
if (drop == "y"):
	coll = db.participant_info
	coll.drop()


for year in years:
	import_to_db(year, db)

open("log.txt","w").close()
print("Загрузка завершена")

regions = db.participant_info.aggregate([
	{ "$match": { "physPTRegName": {"$ne": "null"} } },
	{ "$group": { '_id': "$physPTRegName"} }
])

print(db.participant_info.find().count())

regions = list(regions)
for i in range(len(regions)):
	regions[i] = regions[i]["_id"]


phys_avg_result = dict(zip(regions, [[] for i in regions]))


for year in years:
	phys_avg = db.participant_info.aggregate([
	   { "$match": { "physTestStatus": "Зараховано",  "year": year} },
	   { "$group": { '_id': "$physPTRegName", "ball": { "$avg": "$physBall100" } } }
	])
	phys_avg = list(phys_avg)
	for doc in phys_avg:
		print(doc['ball'])
		phys_avg_result[doc['_id']].append(str(round(doc['ball'], 3)))










csv_data = [(key + ";" + ";".join(value)) for key, value in phys_avg_result.items()]
'''
with open("Result.csv", "w") as result_file:
	result_file.write("Регіон;" + ";".join([("Середній бал по фізиці в " + year + " році") for year in years]))
	for item in csv_data:
		result_file.write("\n")
		result_file.write(item)
print("Запись завершена")'''
