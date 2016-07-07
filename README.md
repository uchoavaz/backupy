# BACKUPY

Este projeto automatiza o processo de backup do SGBD Postgres e quaisquer tipos de pastas que o usuário deseje passar para um
servidor remoto dentro da sua rede privada, além de enviar o log do processo para o e-mail de quem desejar.

Ferramentas necessárias:
-------------------------

  - Ubuntu ou Debian
  - python > 2.7
  - Postgres > 0.8

Configurando o Projeto
--------------------------

Edite o arquivo "run.py" com a configurações desejadas:
Ex:

  - BKP_CONFIG
      - 'user_password': "*senha da maquina local",
      - 'pg_user': '*usuario do postgres',
      - 'host_machine': '*localhost',
      - 'db_password': '*senha do postgres',
      - 'port': '5432',
      - 'local_destiny_folder': '*crie uma pasta local e ponha o seu caminho',
      - 'server_mount_folder': '*pasta do servidor remoto de backup que será montada',
      - 'DB_IGNORED': [*lista de nomes de bancos que serão excluidos do backup],
      - 'server_user': '*usuário do servidor remoto',
      - 'server_address': '*endereço do servidor remoto',
      - 'server_password': '*senha do servidor remoto',
      - 'days_delete': *dias que serão contados quando o backupy irá contar na hora de deletar arquivos antigos,
      - 'folders_to_pass':
      [
          '*lista de caminhos de pastas que serão feitos backups'
      ],
      - 'send_email_success': *True para disparar email para backups de sucesso e False para backups com erros
  
  - EMAIL_CONFIG 
  
      - 'recipient_list': [*lista de emails que irao receber o log],
      - 'email_host': '*email servidor',
      - 'email_password': '*senha do email servidor',
      - 'host': 'smtp.gmail.com',
      - 'domain': 'gmail.com',
      - 'port': '465',
      - 'local_password': '*senha da maquina local'

Rodando o projeto
---------------------
    python run.py
