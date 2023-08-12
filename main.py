from Algorithms import toolsClass
from DataBaseManager import OperationalDataBase, LogFiles
from DataGetter import DataGetter
from GlobalFunctions import UserEmailHandler
import os


class Worker:
    folderFiles = os.path.join(
        '/home', 'fernando', 'Área de Trabalho',
        'Projeto_Estacao', 'csv_estacao'
    )

    def __init__(self) -> None:
        self.retriever = toolsClass.FileRetriever(self.folderFiles)

    def run(self) -> None:
        self.retriever._findFiles()
        files = self.retriever.getFoundFiles()
        for currentFile in files:
            try:
                dE = toolsClass.DataExtractor()
                # dP = toolsClass.DataProcessor()
                dE.extractedDailyData(currentFile, -1)
                for i in dE.getExtractData():
                    print(i, '\n')
                del dE
                break
            except Exception as e:
                print(e)


if __name__ == '__main__':
    exec_ = Worker()
    exec_.run()
