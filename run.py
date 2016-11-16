
from postgres_bkp import Pg_Backup

BKP_CONFIG = {
    'db_name_record': 'core_backup',
    'total_steps': 5.0,
    'db_name_log_record': 'core_backuplog',
    'user_password': 'g3n0m1k@',
    'pg_user': 'genomika',
    'host_machine': 'localhost',
    'db_password': 'g3n3t1k@',
    'port': '5432',
    'local_destiny_folder': '/home/genomika/temp/',
    'server_mount_folder': '/genomikalab/Backups/Bancos/',
    'DB_IGNORED': [
        'template1',
        'template0',
        'postgres'
    ],
    'server_user': 'genomika',
    'server_address': '172.16.225.15',
    'server_password': 'g3n3t1c@',
    'days_delete': 7,
    'folders_to_pass':
    [
        '/var/www/genomika-collaboration.gensoft/genomika_soft/media',
        '/home/genomika/seeddms_db'
    ],
    'send_email_success': True
}

EMAIL_CONFIG = {
    'recipient_list': ['joao.victor@genomika.com.br', 'suporte@genomika.com.br'],
    'email_host': 'notificacoes@genomika.com.br',
    'email_password': 'g3n0m1k@',
    'host': 'smtp.gmail.com',
    'domain': 'gmail.com',
    'port': '465',
    'local_password': BKP_CONFIG['user_password']
}

bkp = Pg_Backup(BKP_CONFIG, EMAIL_CONFIG)
bkp.backup()
