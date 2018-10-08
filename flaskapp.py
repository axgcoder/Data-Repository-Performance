from flask
import Flask, render_template, request, url_for
import os
import time
import MySQLdb
import csv

import hashlib
import memcache

# from pymemcache.client.base
import Client

app = Flask(__name__)

AWS_ACCESS_KEY_ID = 'xxx'
AWS_SECRET_ACCESS_KEY = 'xxxx'
region = 'us-east-2'
UPLOAD_PATH = os.path.dirname(os.path.abspath(__file__))

@app.route('/')
def main(): #connect to DB
db = connectDB()
query = "SELECT * FROM a3data.uszipcodes ORDER BY RAND() LIMIT 1000".encode()## memcache
memc = memcache.Client(['xx.xx.cfg.use2.cache.amazonaws.com:11211'], debug = 0)
hash = hashlib.md5(query).hexdigest()# hash query with md5
cursor = db.cursor()
t0 = time.time()
cursor.execute(query)# t1 = time.time()
starttime = int(round(time.time() * 1000))
data = cursor.fetchall()
rowcount = cursor.rowcount
memc.set(hash, data)

endtime = int(round(time.time() * 1000))
totaltime = endtime - starttime# totaltime = t1 - t0
db.close()
cursor.close()# print("Updated memcached with MySQL data")

return render_template('index.html', data = data, totaltime = totaltime)# return render_template('index.html')

@app.route('/memc', methods = ['GET', 'POST'])
def getmemcache():
  query = "SELECT * FROM earthquakes.all_month ORDER BY RAND() LIMIT 1000".encode()
memc = memcache.Client(['a3mem.kqt2ks.cfg.use2.cache.amazonaws.com:11211'], debug = 0)
print("Connected to memc")# memcache
hash = hashlib.md5(query).hexdigest()# hash query with md5
t0 = time.time()
data = memc.get(hash)
t1 = time.time()
totaltime = t1 - t0
print("Calculated total time")
return render_template('index.html', data = data, totaltime = totaltime)

@app.route('/conditions', methods = ['GET', 'POST'])
def conditions():
  db = connectDB()
cond1 = request.args.get('where1')
cond2 = request.args.get["where2"]
oper = request.args.get["operator"]
if cond1:
  query = "SELECT * FROM earthquakes.all_month where " + cond1 + " ORDER BY RAND() LIMIT 1000 "
args = (cond1)
cursor = db.cursor()
t0 = time.time()
try:
cursor.execute(query)
t1 = time.time()
totaltime = t1 - t0
data = cursor.fetchall()

db.close()
cursor.close()

except Exception as e:
  print(e)
else :
  return "Invalid"
return render_template('index.html', data = data, totaltime = totaltime)

@app.route('/updatedata', methods = ['GET', 'POST'])
def updatedata():
  tablename = "earthquakes"

cond1 = request.form['where1']
cond2 = request.form["where2"]
oper = request.form["operator"]
set = request.args.get["setdata"]

if cond2 != "":
  query = "UPDATE " + tablename + " SET " + set + " WHERE " + cond1 + " " + oper + " " + cond2
else :
  query = "UPDATE " + tablename + "SET " + set + "  WHERE" + cond1

t0 = time.time()
t1 = time.time()
totaltime = t1 - t0
print("Calculated total time")

db = connectDB()
cursor = db.cursor()
cursor.execute(query)

return render_template('update.html', totaltime = totaltime)

##### select

@app.route('/selectdata', methods = ['GET', 'POST'])
def selectdata():

  cond1 = request.form['where1']

cond2 = request.form["where2"]
opr = request.form["operator"]
memstatus = request.form["memstatus"]

tablename = "earthquakes"
query = "SELECT * FROM " + tablename + " WHERE " + cond1 + " " + opr + " " + cond2# query = "Select * FROM " + tablename + " WHERE (latitude BETWEEN " + latfrom + " AND " + latto + ") OR (Longitude BETWEEN " + longfrom + " AND " + longto + ")"#
query = "Select * FROM " + tablename + " WHERE ((latitude BETWEEN " + latfrom + " AND " + latto + ") OR (Longitude BETWEEN " + longfrom + " AND " + longto + ")) AND CountryCode='" + cc + "'"

if memstatus == "yes":
  query = query.encode()
hash = hashlib.md5(query).hexdigest()
memc = memcache.Client(['a3mem.kqt2ks.cfg.use2.cache.amazonaws.com:11211'], debug = 0)
t0 = time.time()
data = memc.get(hash)
t1 = time.time()
totaltime = t1 - t0
else :
  query = query.encode()
db = connectDB()
cursor = db.cursor()
t0 = time.time()
cursor.execute(query)
t1 = time.time()
data = cursor.fetchall()
totaltime = t1 - t0

db.close()
cursor.close()

memc = memcache.Client(['xx.kqt2ks.cfg.xx.cache.amazonaws.com:11211'], debug = 0)
hash = hashlib.md5(query).hexdigest()# hash query with md5
memc.set(hash, data)

return render_template('select.html', data = data, totaltime = totaltime)

@app.route('/createdata', methods = ['GET', 'POST'])
def createdata():

  first = True
starttime = int(round(time.time() * 1000))
file = request.files['inputFile']
file_name = file.filename

try:
myfile = open(UPLOAD_PATH + '/' + file_name, "r+")# or "a+", whatever you need
print("Opened")
except IOError:
  print("Could not open file! Please close Excel!")

with open(UPLOAD_PATH + '/' + file_name, 'rb') as csv_file:
  reader = csv.reader(csv_file)
columns = next(reader)
columns = [h.strip() for h in columns]
db = connectDB()
cursor = db.cursor()
if first:
  sql = 'CREATE TABLE IF NOT EXISTS zipcode1 (%s)' % ','.join(['%s text' % column
    for column in columns
  ])
cursor.execute(sql)
first = False

query = 'INSERT INTO zipcode1(zip, state, county,city)' + ' VALUES(%s, %s, %s, %s)'

for row in reader:
  cursor.execute(query, row)
endtime = int(round(time.time() * 1000))
totalexectime = endtime - starttime
cursor.close()
db.commit()
db.close()
return 'Time taken to load table : <b>' + str(totalexectime) + '</b> msecs <br>'

#
return render_template('select.html', totaltime = totaltime)

#### DB CONNECTION
def connectDB():
  host = "xxxx.amazonaws.com"
port = 3306
dbname = "dbname"
user = "user"
password = "password"

db = MySQLdb.connect(host = host, #your host, usually localhost user = user, #your username passwd = password, #your password db = dbname,
  port = port)
return db

###### Routing

@app.route('/updatepage', methods = ['GET', 'POST'])
def updatepage():
  return render_template('update.html')

@app.route('/selectpage', methods = ['GET', 'POST'])
def selectpage():

  return render_template('select.html')

@app.route('/createpage', methods = ['GET', 'POST'])
def createpage():
  return render_template('create.html')

if __name__ == '__main__':
  app.run()