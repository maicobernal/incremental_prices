import pandas as pd
import glob

spacer = '*'*10
path_base = '/opt/airflow/dags/datasets/base/'
path_precio = '/opt/airflow/dags/datasets/prices/'
base_cleaned = '/opt/airflow/dags/datasets/base/cleaned/'
precio_cleaned = '/opt/airflow/dags/datasets/base/cleaned/'

from functions import *


def LoadProducto():
    try:
        df = CleanProducto(FileImporter('producto', 'parquet', path = path_base))
        df.to_csv(f'{base_cleaned}producto_clean.csv', index=False)
        print('Producto Cleaned and Saved')
    except:
        print('Error cleaning Producto')

def LoadSucursal():
    try:
        df = CleanSucursal(FileImporter('sucursal', 'csv', path = path_base))
        df.to_csv(f'{base_cleaned}sucursal_clean.csv', index=False)
        print('Sucursal Cleaned and Saved')
    except:
        print('Error cleaning Sucursal')

def LoadPrecios():
    try:
        df = FolderImporterPrecios(path_precio)
        df.to_csv(f'{precio_cleaned}precios_clean.csv', index=False)
        print('Precios Cleaned and Saved')
    except:
        print('Error cleaning Precios')


def UploadAll():
    try:
        engine = ConnectSQL()
    except:
        print('Error connecting to SQL')

    base_files = glob.glob(f'{base_cleaned}*.csv')
    precio_files = glob.glob(f'{precio_cleaned}*.csv')
    all_files_cleaned = base_files + precio_files

    for filename in all_files_cleaned:
        try:
            df = pd.read_csv(filename)
            df.to_sql(filename.split('/')[-1].split('.')[0].split('_')[0], con=engine, if_exists='replace', index=False)
            print('Successfully uploaded ', filename)
        except:
            print('Error uploading ', filename)
