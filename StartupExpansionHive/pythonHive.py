"""
@Author:  Mohammad Fatha
@Date: 2021-11-17
@Last Modified by: Mohammad Fatha
@Title : Program to insert a .csv file from hdfs into hive database using pyhive library,and perform different query.
"""

from pyhive import hive
from LogHandler import logger
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(".env")

host_name = os.getenv("HOST_NAME")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")


def createConnection():

    """
    Description:
        This method is used to create a connection with hive.
    """
   
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database,  auth='CUSTOM')

    return conn

def create_database(db_name):

    """
    Description:
        This method is used to create a hive database.
    Parameter:
        It takes database name as a parameter for creating database.
    """
   
    try:
        connection = hive.Connection(host=host_name, port=port, username=user, password=password,
                                     auth='CUSTOM')

        cur = connection.cursor()
        cur.execute(f"CREATE DATABASE {db_name}")
        # cur.execute(f"use {db_name}")
        logger.info("Database created successfully")

    except Exception as e:
        logger.error(e)


def create_table():

    """
    Description:
        This method is used to create a table in a hive database.
    """
    
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute(f"use {database}")
        cur.execute("create table startupexpansion (Store_Id int, City string, State string, Sales string, Region string, New_Expansion string, Market_Spend string, Revenue string) row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/Hive_CSV/Startup/' tblproperties('skip.header.line.count'='1')")
        logger.info("Table has been created successfully")

    except Exception as e:
        logger.error(e)

def where_clause():

    """
    Description:
        This method is used to perform where clause query.
    """

    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("select * from startupexpansion where Store_ID > 100")
        print(cur.fetchall())

    except Exception as e:
        logger.error(e)

def sort():
    """
    Description:
        This method is used to perform sort query.
    """
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("select * from startupexpansion sort by Store_ID")
        print("After sorting")
        print(cur.fetchall())

    except Exception as e:
        logger.error(e)

def create_dataframe():

    """
    Description:
        This method is used to create a panda dataframe by doing query with a hive database.
    """
    
    try:
        conn = createConnection()
        df = pd.read_sql("select * from startupexpansion sort by Store_ID",conn)
        print("Sorting using dataframe")
        print(df)

    except Exception as e:
        logger.error(e)

def drop(tbl_name):

    """
    Description:
        This method is used to delete a hive database table.
    """
   
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute(f"drop database {tbl_name}")
        logger.info("Table Deleted Successfully")

    except Exception as e:
        logger.error(e)


# create_database("StartupExpansion_Hive")
# create_table()
# where_clause()
# sort()
create_dataframe()
# drop("test")