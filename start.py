
import csv
import logging
import glob
import os
import mysql.connector
import datetime
from datetime import datetime

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="jay2706",
    database="customer"
)

list_of_files = glob.glob('D:/Incubytes/*')  # * means all if need specific format then *.txt
latest_file = max(list_of_files, key=os.path.getctime)
location = 'D:/Incubytes/'
country = []
res = []


def unique(countrylist):
    for i in countrylist:
        if i not in res:
            res.append(i)


mycursor = mydb.cursor()
mycursor.execute("""DROP TABLE IF EXISTS customer_data""")

# create table
mycursor.execute(
    """CREATE TABLE customer_data (customer_name VARCHAR(255) PRIMARY KEY, customer_id VARCHAR(18),open_date date,last_consulted_date date,vaccination_id VARCHAR(5),dr_name VARCHAR(255),state VARCHAR(5),country VARCHAR(5),dob date,is_active VARCHAR(1))""")

with open(latest_file, mode='r') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter='|')
    next(csv_file)
    for row in csv_reader:
        # print(row[2:])
        country.append(row[9])
        unique(country)
        try:
            cursor = mydb.cursor()
            mySql_insert_query = """INSERT INTO customer_data (customer_name, customer_id, open_date,last_consulted_date,vaccination_id,dr_name,state,country,dob,is_active) VALUES (%s,%s,cast(%s as date),cast(%s as date),%s,%s,%s,%s,cast(%s as date),%s)"""
            cursor.execute(mySql_insert_query, row[2:])
        except mysql.connector.Error as error:
            print("Failed to insert into MySQL table {}".format(error))
    print("Record inserted successfully into Laptop table")

for i in res:
    print(i)
    try:
        cursor = mydb.cursor()
        # Drop existing table
        sqlquery1 = """DROP TABLE IF EXISTS Table_""" + i
        #print(sqlquery1)
        mycursor.execute(sqlquery1)
        # mydb.commit()

        sqlquery2 = """CREATE TABLE Table_"""+i + """(customer_name VARCHAR(255) PRIMARY KEY, customer_id VARCHAR(18),open_date date,last_consulted_date date,vaccination_id VARCHAR(5),dr_name VARCHAR(255),state VARCHAR(5),country VARCHAR(5),dob date,is_active VARCHAR(1))"""
        #print(sqlquery2)
        mycursor.execute(sqlquery2)
    except mysql.connector.Error as error:
        print("Failed to execute MySQL table  query {}".format(error))

for i in res:

    try:
        cursor = mydb.cursor()
        fire = """ select * from customer_data where country='"""+i+"""'"""
        print(fire)
        mycursor.execute(fire)

        for j in mycursor.fetchall():
            sqlquery3 = """INSERT INTO table_"""+i+"""(customer_name, customer_id, open_date,last_consulted_date,vaccination_id,dr_name,state,country,dob,is_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            print(sqlquery3)
            mycursor.execute(sqlquery3, j)
            mydb.commit()
    except mysql.connector.Error as error:
        print("Failed to execute MySQL table  query {}".format(error))

if (mydb.is_connected()):
    cursor.close()
    mydb.close()
    print("MySQL connection is closed")

