import pandas as pd
import glob


spacer = '*'*10
path_base = './datasets/base/'
path_precio = './datasets/prices'

from functions import *


def LoadProducto():
    try:
        df = CleanProducto(FileImporter('producto', 'parquet', path = path_base))
        df.to_csv('./datasets/base/clean/producto_clean.csv', index=False)
        print('Producto Cleaned and Saved')
    except:
        print('Error cleaning Producto')

def LoadSucursal():
    try:
        df = CleanSucursal(FileImporter('sucursal', 'parquet', path = path_base))
        df.to_csv('./datasets/base/clean/sucursal_clean.csv', index=False)
        print('Sucursal Cleaned and Saved')
    except:
        print('Error cleaning Sucursal')

def LoadPrecios():
    try:
        df = FolderImporterPrecios(path_precio)
        df.to_csv('./datasets/prices/clean/precios_clean.csv', index=False)
        print('Precios Cleaned and Saved')
    except:
        print('Error cleaning Precios')


def UploadAll():
    engine = ConnectSQL()
    all_files = glob.glob('./datasets/base/clean/*.csv')
    for filename in all_files:
        try:
            df = pd.read_csv(filename)
            df.to_sql(filename.split('/')[-1].split('.')[0], con=engine, if_exists='replace', index=False)
            print('Successfully uploaded ', filename)
        except:
            print('Error uploading ', filename)
