#!/usr/bin/env python

import json
import sqlalchemy as sa

# connect to the database
engine = sa.create_engine("mysql://codetest:swordfish@database/codetest")
connection = engine.connect()

metadata = sa.schema.MetaData(engine)

# make an ORM object to refer to the table
People = sa.schema.Table('people', metadata, autoload=True, autoload_with=engine)
Places = sa.schema.Table('places', metadata, autoload=True, autoload_with=engine)

# reference query for functions below, (unused)
ref_query = """
select country, 
count(distinct(concat(given_name,family_name,date_of_birth)))
from people
join places
on city = place_of_birth
group by 1
order by 2 desc
"""

# output the table to a JSON file
with open('/data/summary_output.json', 'w') as json_file:
  j = sa.join(Places, People,
         People.c.place_of_birth== Places.c.city)

# concat w/ birthdate in case same named persons  
  concat = sa.func.concat(People.c.given_name,
                          People.c.family_name,
                          People.c.date_of_birth)
  
# distinct in case of duplicate entries
  count_col = sa.func.count(concat.distinct())
  stmt = sa.select(Places.c.country,count_col)\
    .select_from(j).group_by(Places.c.country)\
    .order_by(count_col.desc())
  rows = connection.execute(stmt).fetchall()
  rows = {row[0]: row[1] for row in rows}
  json.dump(rows, json_file, separators=(',', ':'))
