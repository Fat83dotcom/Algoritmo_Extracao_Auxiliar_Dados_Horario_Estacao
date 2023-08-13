'''
Este modulo faz a leitura do email, senha e destinatários de um arquivo
externo, que deve estar na mesma pasta que o executável Python.
'''
import sqlite3


class DBInterfaceConfig:
    def __init__(self, dbName: str) -> None:
        self.dbName = dbName

    def createTableDataBase(self) -> None:
        sql = '''
        CREATE TABLE IF NOT EXISTS data_base
        (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_cadastro TEXT NOT NULL UNIQUE,
        db_name TEXT NOT NULL,
        user TEXT NOT NULL,
        host TEXT NOT NULL,
        port TEXT NOT NULL,
        password TEXT NOT NULL
        )'''
        self.executeSQL(sql)

    def executeSQL(self, sql: str, *args) -> None:
        data = args
        con = sqlite3.connect(self.dbName)
        try:
            with con:
                con.execute(sql, *data)
        except Exception as e:
            raise e
        finally:
            con.close()

    def select(self, sql: str) -> list:
        con = sqlite3.connect(self.dbName)
        try:
            with con:
                cur = con.cursor()
                result = cur.execute(sql)
                return list(result)
        except Exception as e:
            raise e
        finally:
            con.close()

    def selectDBData(self, dbName: str) -> dict:
        try:
            colluns = '"db_name", "user", "host", "port", "password"'
            sql = f'SELECT {colluns} FROM data_base WHERE nome_cadastro="{dbName}"'
            dbChoice: list = self.select(sql)
            keys: list = ["dbname", "user", "host", "port", "password"]
            data: list = [dado for lista in dbChoice for dado in lista]
            result: dict = dict(zip(keys, data))
            return result
        except Exception as e:
            raise e


if __name__ == '__main__':
    pass
