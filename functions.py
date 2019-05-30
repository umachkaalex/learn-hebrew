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

def add_row(table_name, conn_obj, values):
  cols = read_table(table_name, conn_obj).columns.tolist()
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
      

### Get inputs, create row and add it to the table
def create_and_save_input(table_name, conn_obj, lang='RUS'):
  table = pd.read_sql('select * from Hebrew;', con=conn_obj)
  hebr_words = table['Hebrew'].tolist()  
  transl_words = table['Translat'].tolist()
  genus_words = table['Genus'].tolist()  
  
  cell_1 = input(lang_dict[lang]['hebrew']) or ''
  if cell_1 == '':
    print(lang_dict[lang]['finish_add'])
    return False
  
  cell_2 = input(lang_dict[lang]['translation']) or ''
  if cell_2 == '':
    print(lang_dict[lang]['finish_add'])
    return False
  
  cell_3 = input(lang_dict[lang]['translitiration']) or ''
  if cell_3 == '':
    print(lang_dict[lang]['finish_add'])
    return False
  
  cell_4 = 'no_input'
  while cell_4 == 'no_input':
    cell_4 = input(lang_dict[lang]['genus']) or ''
    if cell_4 == '':
      print(lang_dict[lang]['finish_add'])
      return False
    if cell_4 not in lang_dict[lang]['genus_list']:
      print(lang_dict[lang]['genius_error'])      
      cell_4 = 'no_input'
  
  cell_5 = 'no_input'
  while cell_5 == 'no_input':
    cell_5 = input(lang_dict[lang]['type']) or ''
    if cell_5 == '':
      print(lang_dict[lang]['finish_add'])
      return False
    if cell_5 not in lang_dict[lang]['type_list']:
      print(lang_dict[lang]['type_error'])      
      cell_5 = 'no_input'
  
  if  cell_1 in hebr_words:    
    idx = hebr_words.index(cell_1)    
    if cell_2 == transl_words[idx] and cell_4 == genus_words[idx]:
      dupl_err_txt = lang_dict[lang]['dupl_err_txt']      
      print(str(dupl_err_txt[0]) + str(cell_1) + '-' + str(cell_2) + '-'+ str(cell_3) + '-'+ str(cell_4) + '-'+ str(cell_5) + str(dupl_err_txt[1]))
      time.sleep(3)
      return True
      
  check = input(str(lang_dict[lang]['check_add']) + str(cell_1) + '-' + str(cell_2) + '-'+ str(cell_3) + '-'+ str(cell_4) + '-'+ str(cell_5)) or ''
  if check == '':
      print(lang_dict[lang]['finish_add'])
      return False
  
  else:
    add_row(table_name, conn_obj, [cell_1, cell_2, cell_3, cell_4, cell_5])
    return True

### Start loop to add new words until empty input is recieved
def add_words_to_dict(conn_dict, table_name, conn_obj):  
  add_next_row = True
  while add_next_row:
    add_next_row = create_and_save_input(table_name, conn_obj)  
    table = pd.read_sql('select * from '+str(table_name) +';', con=conn_obj)
    print(table.tail(3))
    time.sleep(3)
    clear_output()
   
### Create Connection
def connection_object(conn_dict):
  return pymysql.connect(conn_dict['host'], user=conn_dict['user'], port=conn_dict['port'],passwd=conn_dict['password'],
                         db=conn_dict['dbname'])
