# !/usr/bin/python
# coding=utf-8
# -------------------------------------------------------------
# LB2 Monitoramento - Classe de utilitarios
#
#       Autor: Tiago M Reichert
#       Data Inicio:  02/08/2016
#       Data Release: 02/08/2016
#       email: tiago.miguel@lb2.com.br
#       Versão: v1.0a
#       LB2 Consultoria - Leading Business 2 the Next Level!
#---------------------------------------------------------------

import email
import sys
import imaplib
import psycopg2
from datetime import datetime

__author__ = 'tiagoreichert'


class Utils:

    def __init__(self):
        pass

    @staticmethod
    def db_connection(ip, port, password):
        try:
            conn_string = "host='"+str(ip)+"' port='"+str(port)+"' dbname='postgres' user='postgres' password='"+password+"'"
            conn = psycopg2.connect(conn_string)
            Utils.add_log("Conectado ao banco de dados com sucesso !", 'log_geral.log')
            return conn
        except Exception, e:
            Utils.add_log('Erro ao conectar com o banco de dados '+str(e), 'log_geral.log')

    @staticmethod
    def db_add_message_history(conn, account_id, folder, message_id):
        cursor = conn.cursor()
        cursor.execute("insert into MESSAGE (account_id,folder,message_id) values ('{0}', '{1}', '{2}');".format(str(account_id), str(folder), str(message_id)))

    @staticmethod
    def db_set_account_history(conn, account_id, sucessfull):
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        cursor.execute("select status_id from ACCOUNT where id = {0};".format(str(account_id)))
        if int(cursor.fetchone()[0]) != 4:
            if sucessfull:
                cursor.execute("update ACCOUNT set status_id=3 where id = {0};".format(str(account_id)))
            else:
                cursor.execute("update ACCOUNT set status_id=4 where id = {0};".format(str(account_id)))
            cursor.execute("COMMIT")
        cursor.execute("ROLLBACK")

    @staticmethod
    def db_get_account_history(conn, account_id):
        cursor = conn.cursor()
        cursor.execute("select status_id from ACCOUNT where id = {0};".format(str(account_id)))
        id = cursor.fetchone()[0]
        cursor.execute("select description from STATUS where id = {0};".format(str(id)))
        return cursor.fetchone()[0]

    @staticmethod
    def db_add_total_messages(conn, account, total_msgs, duplicates, without_messageid, log_name):
        cursor = conn.cursor()
        cursor.execute("update ACCOUNT set qtd_message={0} , duplicates={1}, without_messageid={2} where id={3}".
                       format(str(total_msgs), str(duplicates), str(without_messageid), str(account['id'])))

        Utils.add_log('Adicionado total de mensagens do email '+str(account['src_email']), log_name)

    @staticmethod
    def db_get_account_to_migrate(conn, migration_id):
        # Get conta não migrada
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        cursor.execute("select * from ACCOUNT where status_id=1 and migration_id={0} for update;".format(migration_id))
        tmp_acc = cursor.fetchone()
        dict = {}
        if tmp_acc:
            count = 0
            for elt in cursor.description:
                dict[elt[0]] = tmp_acc[count]
                count += 1
            cursor.execute("select * from SERVER where id= {0};".format(str(dict['src_server_id'])))
            tmp_server = cursor.fetchone()
            if tmp_server:
                count = 0
                for elt in cursor.description:
                    if 'id' not in elt[0]:
                        dict[elt[0]] = tmp_server[count]
                    count += 1
                cursor.execute("update ACCOUNT set status_id=2 where id = {0} ;".format(str(dict['id'])))
                cursor.execute("COMMIT")
                return dict
        cursor.execute("ROLLBACK")
        Utils.add_log('Não foi localizado nenhuma conta com status: ABERTA', 'log_geral.log')
        return None


    @staticmethod
    def is_folder_already_migrated(folder, qtd_msg, conn, account, log_name):
        cursor = conn.cursor()
        cursor.execute("select count(*) from MESSAGE where account_id={0} and folder='{1}';".format(str(account['id']), str(folder)))
        qtd_msg_migrated = cursor.fetchone()
        if int(qtd_msg_migrated[0]) == int(qtd_msg):
            Utils.add_log('[INFO] Pasta ' + str(folder) + ' da conta de email ' + str(
                account['src_email']) + ' já foi migrada anteriormente -- Ignorando esta pasta', log_name)
            return True
        return False

    @staticmethod
    def is_email_already_migrated(folder, conn, account, message_id, log_name):
        cursor = conn.cursor()
        cursor.execute("select * from MESSAGE where account_id={0} and folder='{1}' and message_id='{2}';".format(str(account['id']), str(folder), str(message_id)))
        tmp_msg = cursor.fetchone()
        if tmp_msg:
            Utils.add_log('[INFO] Mensagem ' + str(message_id) + 'da pasta '+str(folder)+ \
                  ' do email ' + str(account['src_email']) + ' já foi migrada anteriormente - IGNORADA', log_name)
            return True
        return False

    @staticmethod
    def connect(servidor, porta=993, SSL=True):
        if SSL:
            p = 'com SSL'
            conn = imaplib.IMAP4_SSL(servidor, port=porta)
        else:
            p = 'sem SSL'
            conn = imaplib.IMAP4(servidor, port=porta)
        Utils.add_log('[OK] Conexão com o servidor ' + str(servidor) + ' bem sucedida '+str(p), 'log_geral.log')
        return conn

    @staticmethod
    def format_folders(connection, tipo, log_name):
        result, pastas = connection.list()
        pastas_formatadas = {}
        qtd_pastas = len(pastas)
        if 'gmail' in tipo:
            qtd_pastas -= 1
            for pasta in pastas:
                if '\All' in pasta:
                    qtd_pastas -= 1
                    Utils.add_log('[WARNING] A pasta a seguir não será copiada por ter flag \All: '+pasta+' \n------------------------------------------------------------------', log_name)
                elif '\Noselect' not in pasta:
                    p_src = pasta.split('"/" ')[-1]
                    p_dest = p_src.replace("[Gmail]/", "").replace('/', '\\').replace('"','')
                    if '\Sent' in pasta:
                        pastas_formatadas[p_src]= 'Enviados do GMAIL'
                    elif '\Trash' in pasta:
                        pastas_formatadas[p_src]= 'Lixeira do GMAIL'
                    elif '\Junk' in pasta:
                        pastas_formatadas[p_src]= 'Junk'
                    else:
                        pastas_formatadas[p_src] = p_dest

        elif 'collabserv' in tipo:
            for pasta in pastas:
                if '\All' in pasta:
                    Utils.add_log('[WARNING] Pasta '+pasta+' não será copiada por ter flag \ALL \n------------------------------------------------------------------', log_name)
                else:
                    p = pasta.split('"\\\\" ')[-1]
                    pastas_formatadas[p] = p

        elif 'qmail' in tipo or 'roundcube' in tipo:
            for pasta in pastas:
                p_src = pasta.split(' "." ')[-1]
                p_dest = p_src.replace('.', '\\').replace('"', '')
                if '\Sent' in pasta:
                    pastas_formatadas[p_src] = 'Itens Enviados'
                elif '\Junk' in pasta:
                    pastas_formatadas[p_src] = 'Junk'
                elif '\Trash' in pasta:
                    pastas_formatadas[p_src] = 'Lixeira'
                elif 'INBOX\\' in p_dest:
                    pastas_formatadas[p_src] = p_dest.replace("INBOX\\", '')
                else:
                    pastas_formatadas[p_src] = p_dest

        Utils.add_log('[INFO] Quantidade de Pastas que seram Migradas: ' + str(qtd_pastas)+'', log_name)
        Utils.add_log('------------------------------------------------------------------', log_name)
        return pastas_formatadas, str(qtd_pastas)

    @staticmethod
    def add_log(text, log_name, new_line=True):
        f = open("/worker_log/"+log_name, 'a')
        data_hora = str(datetime.now().strftime("%d/%m/%Y %H:%M")+': ')
        if new_line:
            print data_hora+text
            f.write(data_hora+text + '\n')
        else:
            sys.stdout.write(text)
            f.write(text)
        f.close()

    @staticmethod
    def get_message_header(connection, message):
        typ, data = connection.fetch(message, '(FLAGS INTERNALDATE BODY[HEADER.FIELDS (MESSAGE-ID)])')
        internaldate = str(data).split('INTERNALDATE')[1].split('"')[1]
        flags = (str(data).split('FLAGS (')[1].split(')')[0]).replace('\\\\','\\')
        try:
            message_id = str(data).split('Message-ID: ')[1].split('\\')[0]
        except:
            try:
                message_id = str(data).split('Message-Id: ')[1].split('\\')[0]
            except Exception, e:
                message_id = '[ERROR] GET_HEADER: '+str(data)+'\nERRO: '+str(e)
        message_id = message_id.replace("'", "").replace('"', '')
        return message_id, internaldate, flags

    @staticmethod
    def get_message_info(connection, message):
        typ, data = connection.fetch(message, 'FLAGS')
        flags = str(data[0]).split(' ')[2].replace('(', '').replace(')', '')

        typ, data = connection.fetch(message, 'INTERNALDATE')
        date_time = str(data[0]).split('INTERNALDATE')[1].split('"')[1]

        return flags, date_time

    @staticmethod
    def get_message_id(connection, message):
        typ, data = connection.fetch(message, '(BODY[HEADER.FIELDS (MESSAGE-ID)])')
        msg_str = email.message_from_string(data[0][1])
        message_id = msg_str.get('Message-ID')

        return message_id