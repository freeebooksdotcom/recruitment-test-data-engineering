#!/usr/bin/env python

import csv
import json
import sqlalchemy as sa

# connect to the database
engine = sa.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sa.schema.MetaData(engine)

# make an ORM object to refer to the table
People = sa.schema.Table('people', metadata, autoload=True, autoload_with=engine)
Places = sa.schema.Table('places', metadata, autoload=True, autoload_with=engine)
# read the CSV data file into the table

with open('/data/people.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader: 
    connection.execute(People.insert().values(
      given_name = row[0], family_name = row[1], date_of_birth = row[2], place_of_birth = row[3]))


with open('/data/places.csv') as csv_file:
  reader = csv.reader(csv_file)
  next(reader)
  for row in reader: 
    connection.execute(Places.insert().values(
      city = row[0], county = row[1], country = row[2]))

# output the table to a JSON file
# reference query for sql alchemy   
query = """
select country, count(distinct(concat(given_name,family_name,date_of_birth)))
from people
join places
on city = place_of_birth
group by 1
order by 1
"""

with open('/data/summary_output.json', 'w') as json_file:
  j = sa.join(Places, People,
         People.c.place_of_birth== Places.c.city)
  
  concat = sa.func.concat(People.c.given_name,People.c.family_name,People.c.date_of_birth)
  stmt = sa.select(Places.c.country,sa.func.count(concat)).select_from(j).group_by(Places.c.country)
  rows = connection.execute(stmt).fetchall()
  rows = {row[0]: row[1] for row in rows}
  json.dump(rows, json_file, separators=(',', ':'))
