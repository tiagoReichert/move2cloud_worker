#!/usr/bin/python
# coding=utf-8
# -------------------------------------------------------------
#       LB2 Move To Cloud - Email Migration
#
#       Autor: Tiago M Reichert
#       Data Inicio:  18/11/2016
#       Data Release: 18/11/2016
#       email: tiago.miguel@lb2.com.br
#       Versão: v1.0a
#       LB2 Consultoria - Leading Business 2 the Next Level!
# --------------------------------------------------------------

import sys
import argparse
from utils.movetocloud_utils import Utils
from datetime import datetime


def main():
    p = parse_args()

    # Conectar-se ao servidor target (smart cloud)
    try:
        dest_conn = Utils.connect(servidor='imap.notes.na.collabserv.com', porta=993, SSL=True)
    except:
        Utils.add_log('[ERROR] Não foi possivel se conectar ao servidor SMART CLOUD', 'log_geral.log')
        exit(2)

    total_mensagens_migradas = 0

    # conecta no banco de dados com contas
    db_conn = Utils.db_connection(ip=p.database_ip, port=p.database_port, password=p.database_pasword)

    # Loop infinto enquanto tiver contas abertas para migrar no banco de dados
    while True:

        account = Utils.db_get_account_to_migrate(conn=db_conn, migration_id=p.migration_id)
        if account == None:
            break

        tempo_inicio = datetime.now()
        log_name = str(account['src_email']) + '__' + tempo_inicio.strftime("%Y%m%d-%H%M") + '.log'

        Utils.add_log('[INFO] Arquivo de log: ' + log_name, log_name)
        Utils.add_log('[INFO] Iniciado as ' + str(tempo_inicio.strftime("%Y-%m-%d %H:%M")), log_name)

        src_server = account['address']
        src_port = account['port']
        src_ssl = account['ssl']
        src_server_type = account['type']
        if 'f' in str(src_ssl) or 'F' in str(src_ssl):
            src_ssl = False

        try:
            src_conn = Utils.connect(servidor=src_server, porta=src_port, SSL=src_ssl)
        except:
            Utils.add_log('[ERROR] Não foi possivel se conectar com o servidor source: '+str(src_server), log_name)
            Utils.add_log('... ignorando este servidor e seguindo para o próximo, caso exista...', log_name)
            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
            continue

        src_email = account['src_email']
        src_passwd = account['src_password']
        dest_email = account['dst_email']
        dest_passwd = account['dst_password']
        try:
            dest_conn.login(dest_email, dest_passwd)
            Utils.add_log('[OK] Conectado ao email de destino: '+dest_email, log_name)
        except:
            Utils.add_log('[ERROR] Não foi possivel se conectar ao email de destino: '+str(dest_email), log_name)
            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
            continue
        try:
            src_conn.login(src_email, src_passwd)
            Utils.add_log('[OK] Conectado ao email de origem: ' + src_email, log_name)
        except:
            Utils.add_log('[ERROR] Não foi possivel se conectar ao email de origem: '+str(src_email), log_name)
            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
            continue
        pastas, qtd_pastas = Utils.format_folders(connection=src_conn, tipo=src_server_type, log_name=log_name)

        #todo Criar outro serviço que faz o procedimento abaixo
        # adicionar total de mensagens neste email
        total_msgs = 0
        erro = 0
        ids = []
        for pasta in pastas.keys():
            src_conn.select(mailbox=pasta, readonly=True)
            typ, data = src_conn.search(None, 'ALL')
            msgs = data[0].split()
            total_msgs += len(msgs)
            for msg in msgs:
                message_id, data, flags = Utils.get_message_header(connection=src_conn, message=msg)
                equals = False
                for id in ids:
                    if id == message_id+pasta:
                        equals = True
                if not equals:
                    ids.append(message_id+pasta)
                if 'list index out of range' in message_id:
                    erro += 1
        Utils.db_add_total_messages(conn=db_conn, account=account, total_msgs=total_msgs, duplicates=(total_msgs-len(ids)), without_messageid=erro, log_name=log_name)
        # ---------------------------------------

        # total de pastas
        qtd_pasta = 0
        # total de pastas totalmente migradas com sucesso
        total_pastas = 0
        # Para cada pasta no email source
        for pasta in pastas.keys():

            qtd_pasta += 1
            Utils.add_log("Pasta Atual na Origem: " + str(pasta), log_name)
            src_conn.select(mailbox=pasta, readonly=True)
            typ, data = src_conn.search(None, 'ALL')
            msgs = data[0].split()

            # Verificar se pasta existe, caso não exista cria
            try:
                status, data = dest_conn.select(pastas[pasta])
            except:
                try:
                    dest_conn = Utils.connect(servidor='imap.notes.na.collabserv.com', porta=993, SSL=True)
                except:
                    Utils.add_log('[ERROR] Não foi possivel se reconectar ao servidor SMART CLOUD', log_name)
                    exit(2)
                try:
                    dest_conn.login(dest_email, dest_passwd)
                    Utils.add_log('[OK] Reconectado ao email de destino: ' + dest_email, log_name)
                except:
                    Utils.add_log('[ERROR] Não foi possivel se reconectar ao email de destino: ' + str(dest_email),
                                  log_name)
                    Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                    continue
                try:
                    status, data = dest_conn.select(pastas[pasta])
                except:
                    Utils.add_log('[ERROR] Connection time-out with Smart Cloud', log_name)

            if 'NO' in status:
                dest_conn.create(pastas[pasta])
                Utils.add_log("Criado a pasta: " + str(pastas[pasta]), log_name)

            # Caso pasta já não tenha sido migrado anteriormente
            if not Utils.is_folder_already_migrated(folder=str(pasta), qtd_msg=len(msgs), conn=db_conn, account=account, log_name=log_name):

                Utils.add_log("Pasta Atual no Destino: " + str(pastas[pasta]), log_name)
                Utils.add_log("Quantidade de Mensagens: " + str(len(msgs)), log_name)

                # Percorer todas as mensages da pasta
                qtd_msg = 0
                for msg in msgs:
                    # Pegar Header da mensagem
                    try:
                        message_id, data, flags = Utils.get_message_header(connection=src_conn, message=msg)
                        if '[ERROR]' in message_id and 'list index out of range' not in message_id:
                            Utils.add_log(message_id, log_name)
                            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                            continue
                    except Exception, e:
                        Utils.add_log('[ERROR] GET_HEADER: '+str(e), log_name)
                        Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                        continue

                    qtd_migrated = 0
                    # Verificar se Mensagem já foi Migrada
                    if not Utils.is_email_already_migrated(folder=str(pasta), conn=db_conn, account=account, message_id=message_id, log_name=log_name):

                        # Fetch do Email no servidor source
                        try:
                            status_fetch, message_body = src_conn.fetch(msg, '(RFC822)')
                        except:
                            Utils.add_log('[ERROR] Mensagem '+str(message_id)+' do email '+str(src_email) +\
                                  ' na pasta '+ str(pasta) +' NÃO foi migrada (Não foi possível fazer Fetch da mensagem)', log_name)
                            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                            continue

                        # Append do Email no servidor target
                        try:
                            status_append = dest_conn.append(pastas[pasta], flags, '"'+data+'"', message_body[0][1])[0]
                        except:
                            Utils.add_log('[ERROR] Mensagem ' + str(message_id) + ' do email ' + str(src_email) +\
                                  ' na pasta '+ str(pasta) +' NÃO foi migrada (Não foi possível fazer Append da mensagem)', log_name)
                            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                            continue

                        if 'OK' in status_fetch and 'OK' in status_append:
                            total_mensagens_migradas += 1
                            qtd_msg += 1
                            # Adiciona na tela que mais um email foi migrado
                            Utils.add_log('.', log_name, new_line=False)
                            if qtd_msg % 50 == 0:
                                Utils.add_log('\n['+str(qtd_msg+1)+'/'+str(len(msgs)-qtd_migrated)+'] ', log_name, new_line=False)
                            sys.stdout.flush()

                            # Salva no histórico que este email ja foi migrado
                            Utils.db_add_message_history(conn=db_conn, folder=pasta, account_id=account['id'], message_id=message_id)
                        else:
                            Utils.add_log('[ERROR] Status de envio invalido: MENSAGEM='+str(message_id)+', PASTA='+str(pasta)+', EMAIL='+str(src_email), log_name)
                            Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=False)
                            continue
                    else:
                        qtd_migrated += 1

                Utils.add_log('', log_name)
                Utils.add_log('----------------[ Caixa de Email Migradas '+str(qtd_pasta)+'/'+str(qtd_pastas)+' ]------------------', log_name)

        src_conn.logout()
        dest_conn.logout()

        Utils.db_set_account_history(conn=db_conn, account_id=account['id'], sucessfull=True)

        try:
            src_conn.close()
            Utils.add_log('[OK] Conexão com ' + str(src_server) + ' fechada', log_name)
        except:
            Utils.add_log('[OK] Conexão com '+str(src_server)+' fechada', log_name)

        tempo_final = datetime.now()
        Utils.add_log('\n----------------------[ ESTATISTICAS ]----------------------------', log_name)
        Utils.add_log('Quatidade total de mensagens migradas: ' + str(total_mensagens_migradas), log_name)
        Utils.add_log('Quantidade total de pastas migradas: ' + str(qtd_pastas), log_name)
        Utils.add_log('Iniciado as ' + str(tempo_inicio.strftime("%Y-%m-%d %H:%M")), log_name)
        Utils.add_log('Terminou as ' + str(tempo_final.strftime("%Y-%m-%d %H:%M")), log_name)
        Utils.add_log('[ATENÇÃO] esta conta terminou com o status: ' + Utils.db_get_account_history(conn=db_conn,
                                                                                account_id=account['id']),log_name)

    try:
        dest_conn.close()
        Utils.add_log('[OK] Conexão com smart cloud fechada', 'log_geral.log')
    except:
        Utils.add_log('[OK] Conexão com smart cloud fechada', 'log_geral.log')


def parse_args():
    """
    Método de analise dos argumentos do software.
    Qualquer novo argumento deve ser configurado aqui
    :return: Resultado da analise, contendo todas as variáveis resultantes
    """
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-id', '--migration_id', required=True, action='store',
                        dest='migration_id',
                        help='ID da migração que este worker ira trabalhar')

    parser.add_argument('-ip', '--database_ip', required=False, default='187.18.120.187', action='store',
                        dest='database_ip',
                        help='IP do banco de dados com contas da migração')

    parser.add_argument('-port', '--database_port', required=False, default='28000', action='store',
                        dest='database_port',
                        help='Porta do banco de dados com contas da migração')

    parser.add_argument('-paswd', '--database_password', required=True, action='store',
                        dest='database_pasword',
                        help='Senha do banco de dados com contas da migração')

    return parser.parse_args()

if __name__ == "__main__":
    main()
