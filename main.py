import os
from datetime import datetime
from DataGetter.DataGetter import DBUtils
from Algorithms.toolsClass import DataProcessor, Register
from Algorithms.toolsClass import FileRetriever, DataExtractor
from GlobalFunctions.UserEmailHandler import DBInterfaceConfig
from DataBaseManager.LogFiles import LogErrorsMixin, LogTimeMixin


class DBHandler(DBUtils):
    '''Integra o banco de dados de credenciais com o banco da estação.'''
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

    def checkForCreateTable(self, currentDate: str, dataObj: Register) -> str:
        objDate = dataObj.data_hora
        checkDate = objDate.strftime('%d-%m-%Y')
        if currentDate != checkDate:
            self.initializerTimeTables(checkDate)
        return checkDate


class TableDateHandler:
    '''Guarda o estado das datas.'''
    def __init__(self) -> None:
        self.__currentDate = '01-01-2000'

    def __str__(self) -> str:
        return f'{self.__currentDate}'

    @property
    def currentDate(self) -> str:
        return self.__currentDate

    @currentDate.setter
    def setCurrentDate(self, date: str) -> None:
        self.__currentDate = date

    def checkRegisterDate(self, currentDate: datetime) -> None:
        checkCurrentDate = currentDate.strftime('%d-%m-%Y')
        if checkCurrentDate != currentDate:
            self.setCurrentDate = checkCurrentDate


class Worker(LogErrorsMixin, LogTimeMixin):
    folderFiles = os.path.join(
        '/home', 'fernando', 'Área de Trabalho',
        'Projeto_Estacao', 'csv_estacao'
    )

    def __init__(self) -> None:
        self.retriever = FileRetriever(self.folderFiles)
        self.handlerDates = TableDateHandler()
        self.userDBName = 'EmailUserData.db'
        self.mainDBName = 'Oracle'

    def run(self) -> None:
        try:
            self.registerTimeLogStart()
            self.retriever._findFiles()
            files = self.retriever.getFoundFiles()
            for currentFile in files:
                print(currentFile)
                dE = DataExtractor()
                dE.extractedDailyData(currentFile, -1)
                dP = DataProcessor(dE.getExtractData())
                dbUser = DBHandler(self.userDBName, self.mainDBName)
                startTime = self.snapshotTime()
                for register in dP:
                    try:
                        nameTable: str = dbUser.checkForCreateTable(
                            self.handlerDates.currentDate, register
                        )
                        dbUser.insertDB(register._asdict(), nameTable)
                        self.handlerDates.checkRegisterDate(
                            register.data_hora
                        )
                    except Exception as e:
                        className = self.__class__.__name__
                        methName = self.run.__name__
                        self.registerErrors(className, methName, e)
                del dE
                del dP
                endTime = self.snapshotTime()
                self.registerTimeElapsed(startTime, endTime)
            self.registerTimeLogEnd()
        except Exception as e:
            className = self.__class__.__name__
            methName = self.run.__name__
            self.registerErrors(className, methName, e)


if __name__ == '__main__':
    exec_ = Worker()
    exec_.run()
