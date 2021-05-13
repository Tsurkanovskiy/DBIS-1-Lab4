from pymongo import MongoClient
#import psycopg2
import random
import time
#from config import config
#from sqlcommands import sqlcreate, sqldrop, sqlselect, sqlinsert


import pprint

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
	with open("log.txt") as log_file:
		log = log_file.readline()
		if (log == ""):
			cluster_num = 0
		else:
			line = log.split(";")
			if (int(year)<int(line[1])):
				return 1
			else:
				cluster_num = int(line[0])
	duration = float(time.time())
	with open('Odata' + year + 'File.csv') as csvfile:
		header = csvfile.readline()
		header = header.replace('\n','')
		header = header.replace('"','')
		header = header.split(";")
		header.append("year")

		n = 0
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
			result=db.participant_info.insert_one(participant_info)
			log_file = open("log.txt", "w")
			log_file.write(str(cluster_num) + ";" + year)
			log_file.close()
			n+=1
			if(n==1000):
			    break
			'''if ((n > 0)&((n % 50) == 0)):
				try:
					# Коміт (Не буде потрібний)
					conn.commit()
				except psycopg2.InterfaceError as e:
					log_file = open("log.txt", "w")
					log_file.write(str(cluster_num - 1) + ";" + year)
					log_file.close()
					raise e
				cluster_num+=1
				log_file = open("log.txt", "w")
				log_file.write(str(cluster_num) + ";" + year)
				log_file.close()
			if (random.randint(0, test_fall_chance) == 1):
				print("Потеряно соединение с базой данных")
				# Розірвання зв’язку
				conn.close()
		# Коміт (Не буде потрібний)
		conn.commit()
		duration = round((float(time.time()) - duration), 4)
		with open('upload_time.txt','a') as upload_time:
			upload_time.write('Data from Odata' + year + 'File.csv uploaded in ' + str(duration) + ' seconds\n')'''
	return 0	







try:
	open("log.txt").close()	
except FileNotFoundError:
	open("log.txt","w").close()	

years = ['2019', '2020']

'''test_fall = input("Желеете протестировать сценарий 'падения' базы данных? (y/n)")
if (test_fall == "y"):
	test_fall_chance = int(input("Пожалуйста введите n (вероятность падения базы данных после анализа строчки - 1/n): "))
else:
	test_fall_chance = 0'''
client = MongoClient("mongodb://localhost:27017")
db=client.ZNO_data
# Перевірка на існування таблиці
'''cur.execute("select exists(select * from information_schema.tables where table_name='hist_results')")
if (cur.fetchone()[0]):
	drop = input("Желеете удалить базу данных? (y/n)")
	if (drop == "y"):'''
		# Видалення таблиці
coll = db.participant_info
coll.drop()
# Створення таблиці
'''cur.execute("select exists(select * from information_schema.tables where table_name='hist_results')")
if (not (cur.fetchone()[0])):
	cur = sqlcreate(cur)'''

for year in years:
	import_to_db(year, db)

open("log.txt","w").close()
print("Загрузка завершена")

regions = db.participant_info.aggregate([
	{ "$match": { "physPTRegName": {"$ne": "null"} } },
	{ "$group": { '_id': "$physPTRegName"} }
])


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
		phys_avg_result[doc['_id']].append(doc['ball'])
	print(phys_avg_result)










csv_data = [key, ";", ";".join([str(y) for y in x]) for key, value in phys_avg_result.items]
pprint(csv_data)
'''
with open("Result.csv", "w") as result_file:
	result_file.write("Регіон;Найнижчий бал в 2019;Найнижчий бал в 2020")
	for item in csv_data:
		result_file.write("\n")
		result_file.write(item)
print("Запись завершена")'''
