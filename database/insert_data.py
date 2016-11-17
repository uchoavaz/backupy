
import psycopg2
from decouple import config


class InsertData():
    conn = None
    cur = None
    db_ip = None

    def __init__(self):
        self.init_db_config(config)
        self.cur = self.conn.cursor()

    def init_db_config(self, config):
        try:
            self.db_ip = config('DB_HOST')
            self.conn = psycopg2.connect(
                "dbname='{0}'"
                " user='{1}' host='{2}' password={3}".format(
                    config('DB_NAME'),
                    config('DB_USER'),
                    self.db_ip,
                    config('DB_PASSWORD')
                )
            )
            print ("Conectado no Banco com sucesso!")
        except:
            print ("Erro ao conectar a base de dados")

    def insert(self, db_name, column_value):
        if db_name == 'core_backup':
            self.cur.execute(
                u"INSERT INTO"" core_backup "
                "(name, percents_completed, status, start_backup_datetime, "
                "finish_backup_datetime) VALUES "
                "('{0}', {1}, {2}, {3},{4}) RETURNING id".format(
                    column_value['name'],
                    column_value['percents_completed'],
                    column_value['status'],
                    column_value['start_backup_datetime'],
                    column_value['finish_backup_datetime']
                )
            )
        elif db_name == 'core_backuplog':

            self.cur.execute(
                u"INSERT INTO"" core_backuplog "
                "(backup_id, log, status, log_datetime) VALUES "
                "({0}, '{1}', {2}, {3}) RETURNING id".format(
                    column_value['backup_id'],
                    column_value['log'],
                    column_value['status'],
                    column_value['log_datetime']
                )
            )
        pk = self.cur.fetchone()[0]
        self.conn.commit()

        return pk

    def update(self, db_name, column_value):
        if db_name == 'core_backup':
            self.cur.execute(
                u"UPDATE {0} SET status={1}, percents_completed={2}, "
                "finish_backup_datetime={3} WHERE id={4}".format(
                    db_name,
                    column_value['status'],
                    column_value['percents_completed'],
                    column_value['finish_backup_datetime'],
                    column_value['id'],
                )
            )
        if db_name == 'core_backuplog':
            self.cur.execute(
                u"UPDATE {0} SET status={1}, log='{2}' WHERE id={3}".format(
                    db_name,
                    column_value['status'],
                    column_value['log'],
                    column_value['id'],
                )
            )
        self.conn.commit()

    def query(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def get_ip(self):
        return self.db_ip

    def close_conn(self):
        self.conn.close()
