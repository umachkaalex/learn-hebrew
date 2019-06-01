import pymysql
import pandas as pd
import time
from IPython.display import clear_output
from langdicts import *

### CREATE TABLE
def create_table(conn_dict, table_name, cols, types):
  try:
      connectionObject = pymysql.connect(conn_dict['host'],
                                           user=conn_dict['user'],
                                           port=conn_dict['port'],
                                           passwd=conn_dict['password'],
                                           db=conn_dict['dbname'])


      # Create a cursor object
      cursorObject = connectionObject.cursor()                                     

      # SQL query string
      sqlQuery = 'CREATE TABLE ' + str(table_name) + '('
      
      for i in range(len(cols)):
        sqlQuery = sqlQuery + str(cols[i]) + ' ' + str(types[i])
        if i != len(cols) - 1:
          sqlQuery = sqlQuery+ ', '
      sqlQuery = sqlQuery + ') CHARACTER SET utf8 COLLATE utf8_general_ci'      

      # Execute the sqlQuery
      cursorObject.execute(sqlQuery)

      # SQL query string
      sqlQuery = "show tables"    

      # Execute the sqlQuery
      cursorObject.execute(sqlQuery)

      #Fetch all the rows
      rows = cursorObject.fetchall()

      for row in rows:
          print(row)

  except Exception as e:

      print("Exeception occured:{}".format(e))

  finally:

      connectionObject.close()

# Example
# create_table(conn_dict, 'Hebrew', ['Hebrew','Translat', 'Translit', 'Genus', 'Type'],
#                                   ['varchar(32)', 'varchar(32)', 'varchar(32)', 'varchar(32)', 'varchar(32)'])

### Delete Table
def del_table(conn_dict, table_name):
  try:

      connectionObject = pymysql.connect(conn_dict['host'],
                                           user=conn_dict['user'],
                                           port=conn_dict['port'],
                                           passwd=conn_dict['password'],
                                           db=conn_dict['dbname'])
      # Create a cursor object
      cursorObject        = connectionObject.cursor()                                     

      # SQL query string
      sqlQuery            = 'DROP TABLE ' + str(table_name) + ' CASCADE'   

      # Execute the sqlQuery
      cursorObject.execute(sqlQuery)

      # SQL query string
      sqlQuery            = 'show tables'

      # Execute the sqlQuery
      cursorObject.execute(sqlQuery)

      #Fetch all the rows
      rows                = cursorObject.fetchall()

      for row in rows:
          print(row)

  except Exception as e:
      print("Exeception occured:{}".format(e))

  finally:
      connectionObject.close()

### Read Table
def read_table(table_name, conn_obj):  
  table = pd.read_sql('select * from '+str(table_name)+';', con=conn_obj)   
  return table

def add_row(table_name, conn_obj, values):
  cols = read_table(table_name, conn_obj).columns.tolist()  
  try:    
      
      cursorObject        = conn_obj.cursor()      
            
      # Insert rows into the MySQL Table
      insertStatement = 'INSERT INTO ' + str(table_name) + '('
      
      for i in range(len(cols)):
        insertStatement = insertStatement + str(cols[i])
        if i != len(cols)-1:
           insertStatement = insertStatement + ', '
        else:
           insertStatement = insertStatement + ') VALUES (\''
            
      for i in range(len(values)):
        insertStatement = insertStatement + str(values[i])
        if i != len(cols)-1:
           insertStatement = insertStatement + '\',\''
        else:
           insertStatement = insertStatement + '\')'
      
    
                       
      cursorObject.execute(insertStatement)

      cursorObject.close()
      
  except Exception as e:

      print("Exeception occured:{}".format(e))

  finally:
    
      conn_obj.commit()      
      

### Get inputs, create row and add it to the table
def create_and_save_input(table_name, conn_obj, lang='RUS'):
  if check != '':
      print(lang_dict[lang]['finish_add'])
      return False
  
  else:
    add_row(table_name, conn_obj, [cell_1, cell_2, cell_3, cell_4, cell_5, cell_6])
    return True

### Start loop to add new words until empty input is recieved
def add_words_to_dict(conn_dict, table_name, conn_obj):  
  add_next_row = True
  while add_next_row:
    add_next_row = create_and_save_input(table_name, conn_obj)  
    table = read_table(table_name, conn_obj)
    print(table.tail(3))
    time.sleep(3)    
    clear_output()
   
### Create Connection
def connection_object(conn_dict):
  return pymysql.connect(conn_dict['host'], user=conn_dict['user'], port=conn_dict['port'],passwd=conn_dict['password'],
                         db=conn_dict['dbname'])
