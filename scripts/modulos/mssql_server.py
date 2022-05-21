import sqlalchemy
import pyodbc


class MSSQL:
    host: str
    name: str
    user: str
    password: str
    port: int = 1433
    driver: str = '{ODBC Driver 17 for SQL Server}'
    driver_engine: str
    __conn__ = None

    def engine(self):
        sc = f'mssql+pyodbc://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}?driver={self.driver_engine}'
        return sqlalchemy.create_engine(sc)

    def open_con(self):
        sc = f'DRIVER={self.driver};SERVER={self.host};DATABASE={self.name};UID={self.user};PWD={self.password}'
        self.__conn__ = pyodbc.connect(sc)

    def close_con(self):
        if self.__conn__:
            self.__conn__.close()


class Querys(MSSQL):
    
    def delete_script(self, table: str, schema: str, column: str, condition: str):
        query = f"DELETE FROM [{schema}].[{table}] WHERE {column} = '{condition}'"
        cursor = self.__conn__.cursor()
        with cursor.execute(query):
            print("Registros eliminados")
