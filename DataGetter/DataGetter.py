from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from DataBaseManager.OperationalDataBase import DadoHorario, DataBasePostgreSQL
from DataBaseManager.OperationalDataBase import GerenciadorTabelas


class DBUtils:
    def __init__(self, dadosBD: dict) -> None:
        self.dadosBD = dadosBD
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.dB = DataBasePostgreSQL(self.dadosBD)
        self.dDH = DadoHorario(self.dB)
        self.dGT = GerenciadorTabelas(self.dB)

    def inicializadorTabelasHorarias(self) -> None:
        try:
            tableName = self.dGT.nameTableGenerator()
            self.dGT.execInsertTable(
                (tableName,),
                table='gerenciador_tabelas_horarias',
                collumn=('data_tabela', )
            )
            fKey: int = self.dGT.getForeignKey()
            self.executor.submit(
                self.dDH.execCreateTable,
                fKey, tableName=tableName, schema='tabelas_horarias'
            )
        except Exception:
            ...

    def checkRegisterDate(self, currentDate: datetime) -> None:
        if now.hour == 0 and now.minute == 0 and now.second == 0:
            self.inicializadorTabelasHorarias()

    def insereBancoDados(self, dataInsert: dict) -> None:
        tableName = self.dGT.nameTableGenerator()
        self.executor.submit(
            self.dDH.execInsertTable,
            dataInsert,
            table=tableName,
            collumn=(
                'data_hora', 'umidade', 'pressao', 'temp_int', 'temp_ext'
            ),
            schema='tabelas_horarias'
        )
