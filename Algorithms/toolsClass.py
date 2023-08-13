import os
import csv
from itertools import groupby
from datetime import datetime, timedelta
from collections import namedtuple
from DataBaseManager.LogFiles import LogErrorsMixin


class FileRetriever(LogErrorsMixin):
    '''
        Busca arquivos, manipula caminhos e nomes de arquivos.
    '''
    def __init__(self, pathTarget, extension='.csv') -> None:
        self.__foundFiles: list = []
        self.__pathTarget = pathTarget
        self.__extensionFile = extension

    def findYesterdayFile(self, month, year) -> None:
        '''
            Busca o arquivo cujo o mês está na data de ontem.
            Salva o arquivo no atributo self.__foundFiles
            Retorna -> None
        '''
        try:
            fileName = self.__generatorNameFile(month, year)
            self.__foundFiles.append(
                self.findOneFile(fileName)
            )
        except Exception as e:
            className = self.__class__.__name__
            methName = self.findYesterdayFile.__name__
            self.registerErrors(className, methName, e)

    def _findFiles(self) -> None:
        '''
            Busca todos os arquivos cujo a extensão foi definida na pasta.
            Salva o caminho dos arquivos no atributo self.__foundFiles.
            Retorna -> None.
        '''
        try:
            self.__foundFiles = [
                os.path.join(root, targetFile)
                for root, _, file_ in os.walk(self.__pathTarget)
                for targetFile in file_
                if self.__extensionFile in targetFile
            ]
        except Exception as e:
            className = self.__class__.__name__
            methName = self._findFiles.__name__
            self.registerErrors(className, methName, e)

    def findOneFile(self, fileName: str | None) -> str | None:
        '''
            Busca um arquivo na pasta definida pelo seu nome.
            Retorna o caminho do arquivo se ele existir.
        '''
        try:
            for root, _, file_ in os.walk(self.__pathTarget):
                for targetFile in file_:
                    if fileName in targetFile:
                        return str(os.path.join(root, targetFile))
            return 'Arquivo não encontrado.'
        except Exception as e:
            className = self.__class__.__name__
            methName = self.findOneFile.__name__
            self.registerErrors(className, methName, e)

    def __generatorNameFile(self, month, year) -> str | None:
        '''
            Atributo de classe.
            Gera o nome de um arquivo baseado em seu mês e ano.
            Retorna o nome do arquivo.
        '''
        try:
            nameFile = os.path.join(
                f'{month}_{year}_log{self.__extensionFile}'
            )
            return nameFile
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__generatorNameFile.__name__
            self.registerErrors(className, methName, e)

    def getFoundFiles(self):
        '''
            Retorna o atributo self.__foundFiles.
        '''
        try:
            if self.__foundFiles:
                for files in self.__foundFiles:
                    yield files
            else:
                raise Exception('Arquivos não encontrdos')
        except Exception as e:
            className = self.__class__.__name__
            methName = self.getFoundFiles.__name__
            self.registerErrors(className, methName, e)


class DataExtractor(LogErrorsMixin):
    '''
        Extrai os dados brutos dos arquivos e agrupa-os por dia.
    '''
    def __init__(self) -> None:
        self.__extractData: list = []

    def dataExtract(self, pathFile: list) -> None:
        '''
            Extrai dados de arquivos .csv.
            Paramtros:
            file -> Nome do arquivo.
            O dados são salvos no atributo self.__extractData
            pelo metodo de classe self.__groupbyDataByDate
        '''
        try:
            PATH_CSV = pathFile  # type: ignore
            with open(PATH_CSV, 'r', encoding='utf-8') as myCsv:
                reader = csv.reader((line.replace('\0', '') for line in myCsv))
                self.__groupbyDataByDate(reader)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.dataExtract.__name__
            self.registerErrors(className, methName, e)

    def extractedReversedDailyData(self, pathFile: str, dateTarget: int):
        '''Informe o caminho do arquivo e a data da extração.
        -1 le o arquivo inteiro.
        Retorna os dados
        retirados do arquivo'''
        try:
            with open(pathFile, 'r', encoding='utf-8') as file:
                dataFile: list = [
                    x.replace('\0', '') for x in file.readlines()
                ]
                extractDataTarget: list = []
                for data in dataFile[-1::-1]:
                    datas = data[:3].strip()
                    if datas == '':
                        continue
                    if dateTarget == -1:
                        extractDataTarget.append(
                            data.strip().split(',')
                        )
                    elif int(datas) > dateTarget:
                        ...
                    elif int(datas) == dateTarget:
                        extractDataTarget.append(
                            data.strip().split(',')
                        )
                    else:
                        break
            self.__groupbyDataByDate(extractDataTarget)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.extractedDailyData.__name__
            self.registerErrors(className, methName, e)

    def extractedDailyData(self, pathFile: str, dateTarget: int):
        '''Informe o caminho do arquivo e a data da extração.
        -1 le o arquivo inteiro.
        Retorna os dados retirados do arquivo'''
        try:
            with open(pathFile, 'r', encoding='utf-8') as file:
                dataFile: list = [
                    x.replace('\0', '') for x in file.readlines()
                ]
                extractDataTarget: list = []
                for data in dataFile:
                    datas = data[:3].strip()
                    if datas == '':
                        continue
                    if dateTarget == -1:
                        extractDataTarget.append(
                            data.strip().split(',')
                        )
                    elif int(datas) > dateTarget:
                        ...
                    elif int(datas) == dateTarget:
                        extractDataTarget.append(
                            data.strip().split(',')
                        )
                    else:
                        break
            self.__groupbyDataByDate(extractDataTarget)
        except Exception as e:
            className = self.__class__.__name__
            methName = self.extractedDailyData.__name__
            self.registerErrors(className, methName, e)

    def __groupbyDataByDate(self, iterable):
        '''
            Agrupa os dados por data.
            Salva os dados no atributo self.__stractData
        '''
        def __extractKey(listTarget):
            try:
                return listTarget[0][:20]
            except Exception as e:
                className = self.__class__.__name__
                methName = __extractKey.__name__
                self.registerErrors(className, methName, e)
        try:
            groups = groupby(iterable, key=__extractKey)
            for date, data in groups:
                self.__extractData.append((date, [
                    (
                        round(float(value[1]), 2),
                        round(float(value[2]), 2),
                        round(float(value[3]), 2),
                        round(float(value[4]), 2)
                    )
                    if
                    value[1] and value[2] and value[3] and value[4] != ''
                    else (0, 0, 0, 0)
                    for value in data
                ]))
        except Exception as e:
            className = self.__class__.__name__
            methName = self.__groupbyDataByDate.__name__
            self.registerErrors(className, methName, e)

    def getExtractData(self) -> list:
        '''
        Retorna o atributo self.__extractData
        '''
        return self.__extractData


Register = namedtuple(
    'Register', [
        'data_hora', 'umidade', 'pressao', 'temp_int', 'temp_ext'
    ]
)


class DataProcessor(LogErrorsMixin):
    '''Processa os dados e prapara-os para entrar no banco de dados.'''
    def __init__(self, dataTarget: list) -> None:
        super().__init__()
        self.dataProcessed = [
            Register(
                datetime.strptime(dt[0], '%d %b %Y %H:%M:%S'),
                umi, press, tmpInt, tmpExt
            )
            for dt in dataTarget
            for umi, press, tmpInt, tmpExt in dt[1]
        ]

    def __getitem__(self, position) -> Register:
        return self.dataProcessed[position]


class DailyDate(LogErrorsMixin):
    '''Manipula datas.'''
    def __init__(self) -> None:
        self.__todayDate: datetime = datetime.now()

    def yesterdayDate(self) -> datetime:
        '''Retorna a data de ontem.'''
        return self.__todayDate - timedelta(1)

    def getTodayDate(self) -> datetime:
        '''Retorna o atributo __todayDate, contendo a data atual.'''
        return self.__todayDate

    def extractDay(self, date: datetime) -> str:
        '''Retorna o dia da data informada.'''
        dd = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcDay = dd.strftime('%d')
        return extratcDay

    def extractMonth(self, date: datetime) -> str:
        '''Retorna o mês da data informada.'''
        dm = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcMonth = dm.strftime('%m')
        return extratcMonth

    def extractYear(self, date: datetime) -> str:
        '''Retorna o ano da data informada.'''
        dt = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S.%f')
        extratcYear = dt.strftime('%Y')
        return extratcYear
