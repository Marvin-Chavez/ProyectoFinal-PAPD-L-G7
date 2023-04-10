# Importación liberías 
import sys

original_stdout = sys.stdout

# Escribe todas las salidas en un  archivo Sumario.txt para ver los resultados de los pasos. 
sys.stdout = open('Sumario.txt', 'w')


import pandas as pd
import numpy as np
import datetime
import boto3
import psycopg2
import configparser
rdsIdentifier = 'proyecto-db-l' #nombre de la instancia
config = configparser.ConfigParser()
config.read('escec.cfg')
aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')

rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")

#Creación de la intancia en AWS
try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('RDS', 'DB_USER'),
            MasterUserPassword=config.get('RDS', 'DB_PASSWORD'),
            Port=int(config.get('RDS', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

import instruccionessql

#logueo de usuario de conexión y creacion de Tablas en SQL Postgre para el escenario
try:
    db_conn = psycopg2.connect(
        database=config.get('RDS', 'DB_NAME'), 
        user=config.get('RDS', 'DB_USER'),
        password=config.get('RDS', 'DB_PASSWORD'), 
        host=RDS_HOST,
        port=config.get('RDS', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(instruccionessql.DDL_QUERY)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


#Alojamiento en AWS S3 para los archivos 
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)
for bucket in s3.buckets.all():
    S3_BUCKET_NAME = bucket.name
    print(bucket.name)

#Lectura del bucket de S3 de los 6 archivos base
S3_BUCKET_NAME = 'bkproyecto'
remoteFileList = []
for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
    remoteFileList.append(objt.key)

remoteFileList


import io

#Lectura del archivo categorias. 
target_file = "Categorias.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_categorias = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  

df_categorias.head()


import io

#Lectura del archivo Direcciones. 
target_file = "Direcciones.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_direcciones = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  

df_direcciones.head()


import io

#Lectura del archivo estado. 
target_file = "Estado.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_estado = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  

df_estado.head()


import io

#Lectura del archivo Metodos de Pago.
target_file = "MetodosPago.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_MetodosPago = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  

df_MetodosPago.head()

import io

#Lectura del archivo Pedidos u Ordenes es el archivo que contiene la Factable.
target_file = "Pedidos.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_pedido = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  # Salir del bucle for una vez que se haya encontrado y leído el archivo

df_pedido.head()




import io

#Lectura del archivo Productos.
target_file = "Producto.xlsx"

for remoteFile in remoteFileList:
    if remoteFile == target_file:
        try:
            file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
            data = file['Body'].read()
            df_producto = pd.read_excel(io.BytesIO(data), engine='openpyxl')
        except Exception as ex:
            print("No es un archivo.")
            print(ex)
        break  # Salir del bucle for una vez que se haya encontrado y leído el archivo

df_producto.head()


#Defición de la función que leera los datos de un DataFrame y lo enviara a un tabla de SQL Postgre
def insertDataToSQL(data_dict, table_name):
     postgres_driver = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""    
     df_data = pd.DataFrame.from_records(data_dict)
     try:
          response = df_data.to_sql(table_name, postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
     except Exception as ex:
          print(ex)

#Realiza la inserción de datos en el servidor de SQL Postgre
insertDataToSQL(df_direcciones, 'addresses')

insertDataToSQL(df_producto, 'products')

insertDataToSQL(df_pedido, 'orders')

#Driver para la lectura de datos en Postgre SQL
postgres_driver = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}""" 

sql_query = 'SELECT order_date FROM orders;'
dimDate = pd.read_sql(sql_query, postgres_driver)
dimDate.head()


dimDate['year'] = pd.DatetimeIndex(dimDate['order_date']).year
dimDate['month'] = pd.DatetimeIndex(dimDate['order_date']).month
dimDate['quarter'] = pd.DatetimeIndex(dimDate['order_date']).quarter
dimDate['day'] = pd.DatetimeIndex(dimDate['order_date']).day
dimDate['week'] = pd.DatetimeIndex(dimDate['order_date']).week
dimDate['dayofweek'] = pd.DatetimeIndex(dimDate['order_date']).dayofweek
dimDate['is_weekend'] = dimDate['dayofweek'].apply(lambda x: 1 if x > 5 else 0)
dimDate.head()

dimDate['order_date_id'] = dimDate['year'].astype(str) + dimDate['month'].apply(lambda x: str(x).zfill(2))
dimDate['order_date_id'] = dimDate['order_date_id'].astype(str) + dimDate['day'].apply(lambda x: str(x).zfill(2))
dimDate.drop_duplicates(inplace=True)
dimDate.head()


df_estado.drop_duplicates(inplace=True)
df_estado.head()


#Seleccionar datos de la tabla products
sql_query = 'SELECT * FROM products;'
df_SqlProduct = pd.read_sql(sql_query, postgres_driver)
df_SqlProduct.head()


df_SqlProduct = df_SqlProduct.merge(df_categorias, how='inner', on='category_id')
df_SqlProduct = df_SqlProduct.rename(columns={'product_id': 'item_id'})
df_SqlProduct.drop_duplicates(inplace=True)
df_SqlProduct.head()

df_MetodosPago.drop_duplicates(inplace=True)
df_MetodosPago.head()


#Seleccionar datos de la tabla orders
sql_query = 'SELECT * FROM orders;'
df_SqlCust = pd.read_sql(sql_query, postgres_driver)
df_SqlCust.head()

selected_columns = ['cust_id', 'ref_num', 'name_prefix','first_name','middle_initial','last_name','gender','age','full_name','email','customer_since' ,'ssn','phone_no','user_name']
df_SqlCust = df_SqlCust[selected_columns]
df_SqlCust.drop_duplicates(inplace=True)
df_SqlCust.head()



#Seleccionar datos de la tabla addresses
sql_query = 'SELECT * FROM addresses;'
df_SqlAddress = pd.read_sql(sql_query, postgres_driver)
df_SqlAddress.drop_duplicates(inplace=True)
df_SqlAddress.head()


#Seleccionar datos de la tabla orders
sql_query = 'SELECT * FROM orders;'
df_SqlFactable = pd.read_sql(sql_query, postgres_driver)
df_SqlFactable.head()


#Agregamos la Id de dates
df_SqlFactable['year'] = pd.DatetimeIndex(df_SqlFactable['order_date']).year
df_SqlFactable['month'] = pd.DatetimeIndex(df_SqlFactable['order_date']).month
df_SqlFactable['day'] = pd.DatetimeIndex(df_SqlFactable['order_date']).day

df_SqlFactable['order_date_id'] = df_SqlFactable['year'].astype(str) + df_SqlFactable['month'].apply(lambda x: str(x).zfill(2))
df_SqlFactable['order_date_id'] = df_SqlFactable['order_date_id'].astype(str) + df_SqlFactable['day'].apply(lambda x: str(x).zfill(2))

df_SqlFactable.drop_duplicates(inplace=True)
df_SqlFactable.head()

#Seleccionamos las columnas que necesitamos para la Factable con comandos de Panda
selected_columns = ['order_id', 'qty_ordered', 'price','value','discount_amount','total','discount_percent','order_date_id','order_date','status_id','item_id' ,'payment_method_id','cust_id','direccion_id']
df_SqlFactable = df_SqlFactable[selected_columns]
df_SqlFactable.drop_duplicates(inplace=True)
df_SqlFactable.head()

rdsIdentifier = 'dw-db'

try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS_MYSQL', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername=config.get('RDS_MYSQL', 'DB_USER'),
            MasterUserPassword=config.get('RDS_MYSQL', 'DB_PASSWORD'),
            Port=int(config.get('RDS_MYSQL', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_DW_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_DW_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)


import CreacionDWquery
import mysql.connector as mysqlC
try:
    myDw = mysqlC.connect(
    host=RDS_DW_HOST, 
    user=config.get('RDS_MYSQL', 'DB_USER'),
    password=config.get('RDS_MYSQL', 'DB_PASSWORD'),
    database=config.get('RDS_MYSQL', 'DB_NAME')
    )

    mycursor = myDw.cursor()
    mycursor.execute(CreacionDWquery.CREATE_DW, multi=True)
    myDw.commit()
    print("Data Warehouse Creado Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)



mysql_driver = f"""mysql+pymysql://{config.get('RDS_MYSQL', 'DB_USER')}:{config.get('RDS_MYSQL', 'DB_PASSWORD')}@{RDS_DW_HOST}:{config.get('RDS_MYSQL', 'DB_PORT')}/{config.get('RDS_MYSQL', 'DB_NAME')}"""  

#insertamos en dimDates
dimDate.to_sql('dimDate', mysql_driver, index=False, if_exists='append')

#insertamos en dimStatus
df_estado.to_sql('dimStatus', mysql_driver, index=False, if_exists='append')

#insertamos en dimProduct
df_SqlProduct.to_sql('dimProduct', mysql_driver, index=False, if_exists='append')

#insertamos en dimPaymentMethod
df_MetodosPago.to_sql('dimPaymentMethod', mysql_driver, index=False, if_exists='append')

#insertamos en dimCust
df_SqlCust.to_sql('dimCust', mysql_driver, index=False, if_exists='append')

#insertamos en dimAddress
df_SqlAddress.to_sql('dimAddress', mysql_driver, index=False, if_exists='append')

#insertamos en Factable
df_SqlFactable.to_sql('Factable', mysql_driver, index=False, if_exists='append')

# cierre de los print
sys.stdout.close()
sys.stdout = original_stdout
