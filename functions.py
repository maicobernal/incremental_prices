import pandas as pd
import numpy as np
import glob

spacer = '*'*10
path_prices = './datasets/prices'
path = './datasets/'

#Import a single file, 
# name = filename
# tipo = extension file,
# path = path to file, 
# spacer = separator for CSV/TXT
# encoding = encoding for CSV/TXT

def FileImporter (name: str, tipo: str, spacer:str = ',', path:str = path, encoding:str = 'utf-8', sheet:int = 0):

    #Raise and error if type of file is not declared
    if tipo == '':
        raise ValueError ('You need to put some extension ir order to import the file')

    #Set the path to the file and extension
    file = path + name + '.' + tipo
    
    #DEBUG
    #print(file)
    
    try:
        #CSV with encoding error
        if tipo == 'csv':
            try:
                df = pd.read_csv(file, sep=spacer, encoding=encoding, low_memory=False)
                return df
            except UnicodeDecodeError as e:
                print('Try a different encoding method for the file', e)
        #XLS/XLSX
        elif tipo == 'xls' or tipo == 'xlsx':
            df = pd.read_excel(file, sheet_name = sheet)
            return df
        
        #JSON
        elif tipo == 'json':
            df = pd.read_json(file)
            return df

        #TXT
        elif tipo == 'txt':
            df = pd.read_csv(file, sep=spacer, encoding='utf-8')
            return df

        #PARQUET
        elif tipo == 'parquet':
            df = pd.read_parquet(file)
            return df
            
    except FileNotFoundError as f:
        print('Error reading file' + str(f))

    finally:
        print('Importing successfully done for ', file)


#Import all files in a folder, path = path to folder, spacer = separator for CSV/TXT




#Normalize strings and encoding for each column
def NormalizeColumn(df, column_name):
    df[column_name] = df[column_name].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df[column_name]



# ETL for producto
delete_columns = ['categoria1', 'categoria2', 'categoria3']

def CleanProducto(df):
    df.drop(columns=delete_columns, inplace=True)
    df['nombre'] = NormalizeColumn(df, 'nombre')
    df['presentacion'] = NormalizeColumn(df, 'presentacion')
    df['marca'] = NormalizeColumn(df, 'marca')
    df['id'] = NormalizeColumn(df, 'id')
    df['id'] = df['id'].str.replace('-', '').astype(int)
    df['nombre'] = df['nombre'].str.split(r"\s\d*\s", regex=True, expand=False).str[0].str.upper()
    return df



# ETL for sucursal
def CleanSucursal(df):
    try:
        df['id'] = df['id'].str.replace('-', '').astype(int)
    except:
        print('id already cleaned')
        pass
    #df['sucursalId'] = df['id'].str.split('-', regex=False, expand=False).str[2]
    df['banderaDescripcion'] = NormalizeColumn(df, 'banderaDescripcion').str.upper()
    df['comercioRazonSocial'] = NormalizeColumn(df, 'comercioRazonSocial').str.upper()
    df['localidad'] = NormalizeColumn(df, 'localidad').str.upper()
    df['direccion'] = NormalizeColumn(df, 'direccion').str.upper()
    return df



# ETL for precios
def CleanPrecios(df):
    #Set order of columns
    col_order = ['precio', 'sucursal_id', 'producto_id']

    #Get percentage of null values for each columns and return it in a list
    checkna = df.isna().sum().div(df.shape[0]).mul(100).round(3).tolist()

    #If NA <1% then drop the column else raise an error
    for i in checkna:
        if i < 1:
            print('Not many null values less than 1%')
            df.dropna(inplace=True)
            break
        else:
            raise ValueError('There are too many null values in the dataset, check it')

    #Clean sucursal_id and keep only real sucursal ID          
    try: 
        df['sucursal_id'] = df['sucursal_id'].str.replace('-', '').astype(int)       
        #df['sucursal_id'] = df['sucursal_id'].str.split('-', regex=False, expand=False).str[2].astype(int)
    except:
        print('sucursal_id already cleaned')
        pass

    #Clean producto ID
    try:
        df['producto_id'] = df['producto_id'].str.replace('-', '').astype(int)
    except:
        print('producto_id already cleaned')
        pass

    #Clean precio
    df['precio'] = df['precio'].apply(pd.to_numeric, errors='coerce')

    return df[col_order]




def FolderImporterPrecios (path:str = path_prices, spacer:str = ',', spacer_txt:str = '|'):

    #Get all files in the folder
    try:
        all_csv = glob.glob(path + "/*.csv")
        all_xls = glob.glob(path + "/*.xls") +  glob.glob(path + "/*.xlsx")
        all_json = glob.glob(path + "/*.json")
        all_txt = glob.glob(path + "/*.txt")
        all_parquet = glob.glob(path + "/*.parquet")

        all_files = all_csv + all_xls + all_json + all_txt + all_parquet
        
        if len(all_files) == 0:
            raise FileNotFoundError('No files found in the folder')

    except:
        print('Error with path or files GLOB ERROR')

    #Make lists for each type of file
    li_csv = []
    li_xls = []
    li_json = []
    li_txt = []
    li_parquet = []


    #Get all CSV in the folder
    if len(all_csv) > 0:
        for filename in all_csv:
            try:
                df = pd.read_csv(filename, sep=spacer, encoding='utf-8', low_memory=False)
                li_csv.append(CleanPrecios(df))
            except:
                df = pd.read_csv(filename, sep=spacer, encoding='utf-16', low_memory=False)
                li_csv.append(CleanPrecios(df))
                print('File imported with utf-16 encoding')
            finally:
                print('Importing successfully done for ', filename)
        precio_final = pd.concat(li_csv, axis=0, ignore_index=True)
    else:
        print('No CSV files found')

    
    #Get all XLS/XLSX in the folder
    if len(all_xls) > 0:
        for filename in all_xls:
            df = pd.read_excel(filename, sheet_name=None)
            if type(df) == dict:
                for key in df:
                    li_xls.append(CleanPrecios(df[key]))
            else:
                li_xls.append(CleanPrecios(df))
        precio_final = pd.concat(li_xls, axis=0, ignore_index=True)
    else:
        print('No XLS/XLSX files found')


    #Get all JSON in the folder
    if len(all_json) > 0:
        for filename in all_json:
            df = pd.read_json(filename)
            li_json.append(CleanPrecios(df))
        precio_final = pd.concat(li_json, axis=0, ignore_index=True)
    else:
        print('No JSON files found')


    #Get all TXT in the folder
    if len(all_txt) > 0:
        for filename in all_txt:
            try:
                df = pd.read_csv(filename, sep=spacer_txt, encoding='utf-8')
                li_txt.append(CleanPrecios(df))
            except:
                print('Error with encoding, not UTF-8 probably', filename)
                df = pd.read_csv(filename, sep=spacer_txt, encoding='utf-16')
                li_txt.append(CleanPrecios(df))
            finally:
                print('Importing successfully done for ', filename)
        precio_final = pd.concat(li_txt, axis=0, ignore_index=True)
    else:
        print('No TXT files found')

    #Get all PARQUET in the folder
    if len(all_parquet) > 0:
        for filename in all_parquet:
            df = pd.read_parquet(filename)
            li_parquet.append(CleanPrecios(df))
        precio_final = pd.concat(li_parquet, axis=0, ignore_index=True)
    else:
        print('No PARQUET files found')

    return precio_final
