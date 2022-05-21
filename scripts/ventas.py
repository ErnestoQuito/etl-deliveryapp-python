import os
import pandas as pd
from datetime import datetime
from modulos.setting import get_setting
from modulos.mssql_server import Querys

# Get variables from file .ini
setting_cliente = get_setting('files.ini', 'VENTAS')

# Read excel
df = pd.read_excel(setting_cliente['path_file'], sheet_name=setting_cliente['name_sheet'])

# Add columns: fecha_carga and nombre_archivo
df['fecha_carga'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df['nombre_archivo'] = os.path.basename(setting_cliente['path_file'])

# Delete by "fecha_carga"
# Get credentials from file .ini
credentials_db = get_setting('db.ini', 'BASEDATOS')
setting_table = get_setting('table.ini', 'VENTAS')
querys = Querys()
querys.host=credentials_db['host']
querys.name=credentials_db['name']
querys.user=credentials_db['user']
querys.password=credentials_db['password']
querys.port=credentials_db['port']
querys.driver=credentials_db['driver']
querys.driver_engine=credentials_db['driver_engine']
querys.open_con()
querys.delete_script(
    table=setting_table['table'],
    schema=setting_table['schema'],
    column=setting_table['column'],
    condition=os.path.basename(setting_cliente['path_file']),
)

# Insert data
df.to_sql(
    setting_table['table'],
    querys.engine(),
    schema=setting_table['schema'],
    if_exists='append',
    chunksize=50,
    index=False
)

querys.close_con()
