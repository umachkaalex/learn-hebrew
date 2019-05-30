import pymysql
import pandas as pd
import time

### Create language texts
lang_dict = {'RUS': {'hebrew': 'введите слово на иврите: ',                     
                     'translation': 'введите перевод: ',                     
                     'translitiration': 'введите транслитерацию: ',
                     'genus': 'введите род (муж, жен): ',
                     'genus_list': ['муж', 'жен'],
                     'type': 'введите часть речи (сущ, глаг, мест, прилаг, нареч, вопрос, союз): ',
                     'type_list': ['сущ', 'глаг', 'мест', 'прилаг',
                                   'нареч', 'вопрос', 'союз', 'доп'],
                     'type_error': 'нет такой части речи',
                     'genus_error': 'нет такого рода',
                     'finish_add': 'закончили',
                     'check_add': 'проверяем: ',
                     'dupl_err_txt': ['слово ', ' есть в словаре']
                     }}



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
def read_table(conn_dict, table_name):
  connectionObject = pymysql.connect(conn_dict['host'],
                                           user=conn_dict['user'],
                                           port=conn_dict['port'],
                                           passwd=conn_dict['password'],
                                           db=conn_dict['dbname'])
   
  table = pd.read_sql('select * from '+str(table_name)+';', con=connectionObject)   
  connectionObject.close()
  
  return table

def add_row(table_name, values):
  cols = read_table(table_name).columns.tolist()
  cell_1, cell_2, cell_3, cell_4, cell_5  
  try:    
      
      cursorObject        = connectionObject.cursor()      
            
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
    
      connectionObject.commit()      
      
