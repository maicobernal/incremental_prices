import pandas as pd
from sqlalchemy import create_engine

# Export files to SQL
# Create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="pythonuser",
                               pw="borito333.",
                               db="lab1"))

query ='''select avg(p.precio) from sucursal as s
join precios as p on (s.id = p.sucursal_id)
where s.id = '91688';'''

try:
    resultado =  pd.read_sql_query(query, engine)
    print('Query executed')
except:
    print('Error executing query')

print('El resultado del QUERY es: ', resultado)