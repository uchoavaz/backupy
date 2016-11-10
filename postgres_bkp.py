
# -*- coding: utf-8 -*-
from database.insert_data import InsertData
from utils import zip_folder
from utils import delete_folder
from utils import get_last_folder_path
from utils import get_last_folder
from utils import delete_old_files
from utils import clear_name
from email import Email
import socket
import subprocess
import time
import os


class Pg_Backup():
    db = None
    config = None
    pk_row = None
    steps_done = []
    zip_folder_path = None
    bkp_folder_path = None
    email_context_success = ''
    email_context_error = ''

    commands = {
        'shell_folder_name': 'shell_commands',
        'shell_mount_file_name': 'mount.sh',
        'check_pg_psw': 'printenv PGPASSWORD',
        'exp_pws': 'echo "localhost:*:*:{0}:{1}" > ~/.pgpass',
        'list_dbs': "echo 'select datname from pg_database' | psql -t -U {0} -h {1}",
        'list_dbs_error': "psql --list  | cut -f1 -d '|' | tail -n +4",
        'bkp': 'pg_dump -h {0} -p {1} -U {2} -F c -b -v -f "{3}" {4}',
        'bkp_error': 'pg_dump -U {0} -w {1} > {2}',
        'rsync': 'echo {0} | sudo -S rsync -r {1} {2}',
        'mount': " echo {0} | sudo -S mount -t cifs '//{1}{2}' '{3}' -o username='{4}',password='{5}',rw,dir_mode=0777,file_mode=0777",
        'umount': 'sudo umount {0}'
    }

    email = {
        'email_subject': "{0}'s backup at {1}",
        'email_context': "--Success--\n{0}\n--Error--\n{1}",
        'error_msg': '- Everything went wrong',
        'success_msg': '- No error'
    }

    def __init__(self, bkp_config, email_config):
        self.db = InsertData()
        self.config = bkp_config
        self.email_config = email_config

    def mount(self, config):
        cmd = self.commands['mount'].format(
            config['user_password'],
            config['server_address'],
            config['server_mount_folder'],
            config['local_destiny_folder'],
            config['server_user'],
            config['server_password'])

        mount = subprocess.call(cmd, shell=True)
        if mount != 0:
            msg = ' Could not mount server'
            self.db.insert(
                self.config['db_name_log_record'], {
                    'backup_id': self.pk_row,
                    'log': msg,
                    'success': False,
                    'log_datetime': 'now()'
                }
            )
            raise Exception(msg)

        msg = 'Mounted with success'
        self.steps_done.append(True)
        self.db.insert(
            self.config['db_name_log_record'], {
                'backup_id': self.pk_row,
                'log': msg,
                'success': True,
                'log_datetime': 'now()'
            }
        )
        self.email_context_success = self.email_context_success \
            + '- {0}\n'.format(msg)

    def umount(self, config):
        try:
            os.chdir(get_last_folder_path(config['local_destiny_folder']))
            cmd = self.commands['umount'].format(
                config['local_destiny_folder'])
            umount = subprocess.call(cmd, shell=True)
            if umount != 0:
                msg = 'Could not umount folder'
                self.db.insert(
                    self.config['db_name_log_record'], {
                        'backup_id': self.pk_row,
                        'log': msg,
                        'success': False,
                        'log_datetime': 'now()'
                    }
                )
                raise Exception(msg)
            msg = 'Umounted with success'
            self.steps_done.append(True)
            self.db.insert(
                self.config['db_name_log_record'], {
                    'backup_id': self.pk_row,
                    'log': msg,
                    'success': True,
                    'log_datetime': 'now()'
                }
            )
            self.email_context_success = self.email_context_success \
                + '- {0}\n'.format(msg)
        except Exception as err:
            self.treat_exception(err)

    def insert_config(self, pg_user, db_password):
        export_cmd = self.commands['exp_pws'].format(
            pg_user, db_password)
        cmd = os.system(export_cmd)

        if cmd != 0:
            raise Exception('Was not possible to set PGPASSWORD')

    def get_db_list(self, pg_user, host_machine):
        try:
            databases = subprocess.Popen(
                self.commands['list_dbs']
                .format(pg_user, host_machine), shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).stdout.readlines()

            if databases == []:
                raise Exception('No databases available for this user or host')
        except Exception:
            databases = subprocess.Popen(
                self.commands['list_dbs_error']
                .format(pg_user, host_machine), shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE).stdout.readlines()
            if databases == []:
                raise Exception('No databases available for this user or host')
        return databases

    def create_bkp_files(self, databases, config):
        bkp_context_success = []
        bkp_context_error = []
        for database in databases:
            db_name = clear_name(database)
            if db_name is not None and db_name not in config['DB_IGNORED']:
                self.create_folder(
                    config['local_destiny_folder'])
                file_name = \
                    db_name + "_bkp_" + time.strftime('%d_%m_%Y') + '.sql'
                path = os.path.join(self.bkp_folder_path, file_name)
                bkp = subprocess.call(
                    self.commands['bkp_error'].format(
                        config['pg_user'],
                        db_name,
                        path
                    ), shell=True
                )

                if bkp != 0:
                    bkp = subprocess.call(
                        self.commands['bkp'].format(
                            config['host_machine'],
                            config['port'],
                            config['pg_user'],
                            path,
                            db_name
                        ), shell=True
                    )

                    if bkp != 0:
                        bkp_context_error.append(db_name)
                    else:
                        bkp_context_success.append(db_name)

                else:
                    bkp_context_success.append(db_name)
        try:
            zip_folder(self.bkp_folder_path)
            delete_folder(self.bkp_folder_path)
        except Exception as err:
            self.treat_exception(err)

        self.zip_folder_path = self.bkp_folder_path + '.zip'
        msg = "Databases backup: {0}".format(','.join(bkp_context_success))
        self.steps_done.append(True)
        self.db.insert(
            self.config['db_name_log_record'], {
                'backup_id': self.pk_row,
                'log': msg,
                'success': True,
                'log_datetime': 'now()'
            }
        )
        self.email_context_success = self.email_context_success \
            + "- {0}\n".format(msg)
        if bkp_context_error != []:
            msg = "No databases backup: {0}".format(','.join(bkp_context_error))
            self.db.insert(
                self.config['db_name_log_record'], {
                    'backup_id': self.pk_row,
                    'log': msg,
                    'success': False,
                    'log_datetime': 'now()'
                }
            )
            self.email_context_error = "- {0}\n".format(
                msg)

    def create_folder(self, folder_path):
        host_name = socket.gethostname()
        folder_name = host_name + "_bkps"
        self.local_path_folder = os.path.join(folder_path, folder_name)
        if not os.path.isdir(self.local_path_folder):
            cmd = subprocess.call(
                'mkdir ' + self.local_path_folder, shell=True)
            if cmd != 0:
                raise Exception("Could not create destiny folder")

        folder_bkp_name = host_name + '_bkp_' + time.strftime('%d_%m_%Y')
        self.bkp_folder_path = os.path.join(
            self.local_path_folder, folder_bkp_name)
        if not os.path.isdir(self.bkp_folder_path):
            cmd = os.system('mkdir ' + self.bkp_folder_path)
            if cmd != 0:
                raise Exception("Could not create backup folder")

    def sync(self, config):
        bkp_context_success = []
        bkp_context_error = []
        for path in config['folders_to_pass']:

            sync = subprocess.call(
                self.commands['rsync']
                .format(
                    config['user_password'],
                    path,
                    config['local_destiny_folder']
                ), shell=True)
            folder_name = get_last_folder(path)
            if sync != 0:
                bkp_context_error.append(folder_name)
            else:
                bkp_context_success.append(folder_name)
        msg = "Folders synced: {0}".format(','.join(bkp_context_success))
        self.steps_done.append(True)
        self.db.insert(
            self.config['db_name_log_record'], {
                'backup_id': self.pk_row,
                'log': msg,
                'success': True,
                'log_datetime': 'now()'
            }
        )
        self.email_context_success = self.email_context_success \
            + '- {0}\n'.format(msg)
        if bkp_context_error != []:
            msg = "Sync with error: {0}".format(','.join(bkp_context_error))
            self.db.insert(
                self.config['db_name_log_record'], {
                    'backup_id': self.pk_row,
                    'log': msg,
                    'success': False,
                    'log_datetime': 'now()'
                }
            )
            raise Exception(' {0}'.format(msg))

    def dispatch_email(self, email_context):
        try:
            subject = self.email['email_subject'].format(
                socket.gethostname(), time.strftime('%d-%m-%Y:%H:%M'))
            email = Email(self.email_config, subject, email_context)
            email.mail()
        except KeyError as error:
            error = "Error to create email! Variable not found: ".format(
                socket.gethostname()) + str(error)

    def treat_exception(self, err):
        self.db.insert(
            self.config['db_name_log_record'], {
                'backup_id': self.pk_row,
                'log': err,
                'success': False,
                'log_datetime': 'now()'
            }
        )
        err = 'Error in {0}:'.format(socket.gethostname()) + str(err)
        self.email_context_error = \
            self.email_context_error + err + '\n'

    def backup(self):
        try:

            column_value = {
                'name': socket.gethostname(),
                'percents_completed': 0,
                'status': 1,
                'start_backup_datetime': 'now()',
                'finish_backup_datetime': 'NULL'
            }
            self.pk_row = self.db.insert(
                self.config['db_name_record'], column_value)

            self.mount(self.config)

            self.insert_config(
                self.config['pg_user'], self.config['db_password'])
            db_list = self.get_db_list(
                self.config['pg_user'], self.config['host_machine'])

            self.create_bkp_files(db_list, self.config)
            folders_deleted = delete_old_files(
                self.config['days_delete'],
                get_last_folder_path(self.bkp_folder_path))

            self.email_context_success = self.email_context_success \
                + '- Old folders deleted: {0}\n'.format(
                    folders_deleted)

            self.sync(self.config)

        except KeyError as err:
            err = "Error in {0}! Variable not found: ".format(
                socket.gethostname()) + str(err)
            print (err)
            self.email_context_error = \
                self.email_context_error + err + '\n'

        except Exception as err:
            self.treat_exception(err)

        finally:
            self.umount(self.config)

            self.db.close_conn()

            email_ctx_error = self.email_context_error
            email_ctx_success = self.email_context_success

            if self.email_context_error == '':
                email_ctx_error = self.email['success_msg']
            if self.email_context_success == '':
                email_ctx_success = self.email['error_msg']

            email_context = self.email['email_context'].format(
                email_ctx_success, email_ctx_error)
            print(email_context)
            if self.config['send_email_success']\
                    or self.email_context_error != '':
                    self.dispatch_email(email_context)
