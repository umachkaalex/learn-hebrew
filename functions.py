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

### create row and add it to the table
def add_row(table_name, conn_obj, values):
  cols = read_table(table_name, conn_obj).columns.tolist()  
  try:    
      
      cursorObject        = conn_obj.cursor()      
            
      # Insert rows into the MySQL Table
      insertStatement = 'INSERT INTO ' + str(table_name) + '('
      
      for i in range(len(cols)):
        col = cols[i]
        insertStatement = insertStatement + str(col)
        if i != len(cols)-1:
           insertStatement = insertStatement + ', '
        else:
           insertStatement = insertStatement + ') VALUES (\''
            
      for i in range(len(values)):
        col = cols[i]
        insertStatement = insertStatement + str(values[col])
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
      

### get noun inputs
def noun_input(conn_obj, table_name='noun', lang='RUS'):
  
  def add_cell(col):    
    cell = input(lang_noun[lang][col]) or 0
    cell = lambda cell: cell[:1].lower() + cell[1:] if cell != 0 else 0    
    return cell
  
  table = pd.read_sql('select * from ' +str(table_name) + ';', con=conn_obj)
  cols = table.columns.tolist()
  values_1 = table[cols[0]].tolist()
  values_2 = table[cols[1]].tolist()
  cells = dict()
  zeros = len(cols)
  row_str = ''
  status = True
  duplicates = 0
  for col in cols:
    cur_cell = add_cell(col)
    
    if cur_cell in values_1 or in values_2:
      duplicates += 1
    if duplicates == 2:
      print(lang_dict[lang]['dupl_err_txt'])
      time.sleep(3)      
      break
      
    if cur_cell == 0:
      zeros -= 1
    else:
      row_str += str(cur_cell) + ' - '        
      
    if zeros == 7:
      cells[col] = cur_cell
    else:      
      status = False
      break
  
  if not status:
    print(lang_dict[lang]['finish_add'])
    time.sleep(3)
    return False
  else:
    check = input(str(lang_dict[lang]['check_add']) + str(row_str)) or ''
    if check != '':
      return False
    else:
      if duplicates < 2:
        add_row(table_name, conn_obj, cells)      
        return True
      else:
        return True

### Start loop to add new words until empty input is recieved
def add_words_to_dict(conn_dict, table_name, conn_obj):  
  add_next_row = True
  while add_next_row:
    add_next_row = noun_input(conn_obj, table_name=table_name, lang='RUS') 
    table = read_table(table_name, conn_obj)
    print(table.tail(3))
    time.sleep(3)    
    clear_output()
   
### Create Connection
def connection_object(conn_dict):
  return pymysql.connect(conn_dict['host'], user=conn_dict['user'], port=conn_dict['port'],passwd=conn_dict['password'],
                         db=conn_dict['dbname'])
