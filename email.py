
from database.insert_data import InsertData
import subprocess
import socket


class Email():
    commands = {
        'permission': 'echo {0} | sudo -S chmod -R 777 /etc/ssmtp',
        'insert_config': 'echo {0} | sudo -S echo "{1}" > /etc/ssmtp/ssmtp.conf',
        'create_context_file': 'echo {0} | sudo -S echo "{1}" > email_context.txt',
        'send_with_context': 'echo {0} | sudo -S ssmtp {1} < email_context.txt'
    }
    conf_var = {
        'root': 'root={0}',
        'mailhub': 'mailhub={0}:{1}',
        'rewriteomain': 'rewriteomain={0}',
        'AuthUser': 'AuthUser={0}',
        'AuthPass': 'AuthPass={0}',
        'FromLineOverride': 'FromLineOverride=YES',
        'UseTLS': 'UseTLS=YES',
    }

    def __init__(self, config, subject, content):
        self.config = config
        self.subject = subject
        self.content = content

    def insert_config(self, config):
        text = '{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n{6}'.format(
            self.conf_var['root'].format(config['email_host']),
            self.conf_var['mailhub'].format(config['host'], config['port']),
            self.conf_var['rewriteomain'].format(config['domain']),
            self.conf_var['AuthUser'].format(config['email_host']),
            self.conf_var['AuthPass'].format(config['email_password']),
            self.conf_var['FromLineOverride'],
            self.conf_var['UseTLS'])

        subprocess.call(
            self.commands['permission']
            .format(config['local_password']), shell=True)

        cmd = \
            (self.commands['insert_config']
                .format(config['local_password'], text))
        config = subprocess.call(cmd, shell=True)
        if config != 0:
            raise Exception('Could not insert email configurations')

    def send_with_context(self, psw, recipient, email_host, subject, content):
        context = ("To:{0}\nFrom:{1}\nSubject:{2}\n\n{3}".format(
            recipient, email_host, subject, content))
        ctx = subprocess.call(
            self.commands['create_context_file'].format(
                psw, context), shell=True)

        if ctx != 0:
            raise Exception('Could not create email context file')

        send = subprocess.call(
            self.commands['send_with_context'].format(
                psw, recipient), shell=True)

        if send != 0:
            raise Exception('Could not send email')

    def send_mail(self, config, subject, content):
        for recipient in config['recipient_list']:
            self.send_with_context(
                config['local_password'],
                recipient, config['email_host'],
                subject,
                content)

    def mail(self):
        try:
            self.insert_config(self.config)
            self.send_mail(self.config, self.subject, self.content)
        except Exception as err:
            error = 'Error in {0}:'.format(socket.gethostname()) + str(err)
            print(error)
        finally:
            delete = subprocess.call('rm email_context.txt', shell=True)
            if delete != 0:
                raise Exception('Could not delete email_context.txt')
