# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 10:25:21 2023

@author : harivonyratefiarison

@dependencies : 
    mysqlclient
    sqlalchemy
    
"""
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Step 2: Create a Model
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(255))
    email = Column(String(255))

# Step 3: Create a Database Connection
# DB URL format '[dbengine]://[user]:[password]@[ip]:[port]/[database]' 
engine = create_engine(DB_URL_STRING)

# Step 4: Create Tables
Base.metadata.create_all(engine)

# Step 5: Insert Data
Session = sessionmaker(bind=engine)
session = Session()

# Insert a user
new_user = User(username='john_doe', email='john@example.com')
session.add(new_user)
session.commit()

'''
    Trying dataframe insertion
'''
import pandas as pd

data = {'id':[1,2,3],'username':["John","Luc", "Steve"],'email':["1@mail","2@mail","3@mail"]}

column_mapping = {
    "id": "user_id",  # Map the "id" column in the DataFrame to the "user_id" column in the table
    "name": "full_name",  # Map the "name" column in the DataFrame to the "full_name" column in the table
}

df = pd.DataFrame(data=data)
df
df.to_sql(name='users', con=engine, if_exists='replace', index=False)

'''   '''
# Assuming df is your DataFrame


# Query the database to verify the insertion
user = session.query(User).filter_by(username='john_doe').first()
print(f'User ID: {user.id}, Username: {user.username}, Email: {user.email}')

# Close the session
session.close()
