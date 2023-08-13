from DataGetter.DataGetter import DBUtils
from Algorithms.toolsClass import DataProcessor
from Algorithms.toolsClass import FileRetriever, DataExtractor
from GlobalFunctions.UserEmailHandler import DBInterfaceConfig
import os


class DBHandler(DBUtils):
    def __init__(self, DBFile: str, DBName: str) -> None:
        self.dbName = DBName
        self.dbFile = DBFile
        try:
            self.interfaceConfig = DBInterfaceConfig(self.dbFile)
            self.interfaceConfig.createTableDataBase()
            self.result = self.interfaceConfig.selectDBData(self.dbName)
            super().__init__(self.result)
        except Exception as e:
            print(e)


class Worker:
    folderFiles = os.path.join(
        '/home', 'fernando', 'Área de Trabalho',
        'Projeto_Estacao', 'csv_estacao'
    )

    def __init__(self) -> None:
        self.retriever = FileRetriever(self.folderFiles)
        self.dbUser = DBHandler('EmailUserData.db', 'PostgreSQL')

    def recorderDataDB(self, data: list) -> None:
        self.dbUser.inicializadorTabelasHorarias()

    def run(self) -> None:
        self.retriever._findFiles()
        files = self.retriever.getFoundFiles()
        for currentFile in files:
            try:
                dE = DataExtractor()
                dE.extractedDailyData(currentFile, -1)
                dP = DataProcessor(dE.getExtractData())
                for i in dP:
                    print(i)
                del dE
                break
            except Exception as e:
                print(e)


if __name__ == '__main__':
    exec_ = Worker()
    exec_.run()
