#!/usr/bin/env python

import csv
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

