import pandas as pd
import numpy as np
from IPython.display import display
import os
import glob
from sqlalchemy import create_engine

spacer = '*'*10
path_base = './datasets/base/'
path_precio = './datasets/prices'

from functions import *

# Import files locally
try:
    producto = CleanProducto(FileImporter('producto', 'parquet', path = path_base))
    print('Producto imported')
except:
    print('Error importing file for producto')

try:
    sucursal = CleanSucursal(FileImporter('sucursal', 'csv', path = path_base))
    print('Sucursal imported')
except:
    print('Error importing file for sucursal')

try:
    precios = FolderImporterPrecios(path_precio)
    print('Precios imported')
except:
    print('Error importing files for precios')


# Export files to SQL
# Create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="pythonuser",
                               pw="borito333.",
                               db="lab1"))

try:
    producto.to_sql('producto', engine, if_exists='append', index=False)
    print('Producto exported')
except:
    print('Error exporting file for producto')

try:
    sucursal.to_sql('sucursal', engine, if_exists='append', index=False)
    print('Sucursal exported')
except:
    print('Error exporting file for sucursal')

try:
    precios.to_sql('precios', engine, if_exists='append', index=False)
    print('Precios exported')
except:
    print('Error exporting files for precios')
