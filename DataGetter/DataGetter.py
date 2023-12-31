from DataBaseManager.OperationalDataBase import DadoHorario, DataBasePostgreSQL
from DataBaseManager.OperationalDataBase import GerenciadorTabelas


class DBUtils:
    def __init__(self, dadosBD: dict) -> None:
        self.dadosBD = dadosBD
        self.dB = DataBasePostgreSQL(self.dadosBD)
        self.dDH = DadoHorario(self.dB)
        self.dGT = GerenciadorTabelas(self.dB)

    def initializerTimeTables(self, date: str) -> None:
        try:
            tableName = self.dGT.nameTableGenerator(date)
            self.dGT.execInsertTable(
                (tableName,),
                table='gerenciador_tabelas_horarias',
                collumn=('data_tabela', )
            )
            fKey: int = self.dGT.getForeignKey(tableName)
            self.dDH.execCreateTable(
                fKey, tableName=tableName, schema='tabelas_horarias'
            )
        except Exception:
            ...

    def insertDB(self, dataInsert: dict, tableName: str) -> None:
        tableName = self.dGT.nameTableGenerator(tableName)
        self.dDH.execInsertTable(
            dataInsert,
            table=tableName,
            collumn=(
                'data_hora', 'umidade', 'pressao', 'temp_int', 'temp_ext'
            ),
            schema='tabelas_horarias'
        )
