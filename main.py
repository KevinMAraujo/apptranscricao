#!/usr/bin/python3
from mysql.connector import connect
from mysql.connector import Error
from mysql.connector import errorcode
from datetime import datetime
from datetime import timedelta
from decouple import config

import whisper
import ffmpeg


'''PARÂMETROS DE CONEXÃO DB '''

DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_NAME = config('DB_NAME')

DB_URL=config('DB_URL')


class Connect(object):

    def __init__(self):
        self.connection = self.bd_connection()
        #self.connection.execute('set max_allowed_packet=67108864')
        # self.cursor = self.connection.cursor()
        self.cursor = self.connection.cursor(buffered=True)

    def bd_connection(self):
        try:
            connection = connect(
                host=DB_HOST,
                user=DB_USER,
                passwd=DB_PASSWORD,
                database=DB_NAME
            )
            print("Banco de dados conectado.")
            return connection

        except Error as er:
            if er.errno == errorcode.ER_BAD_DB_ERROR:
                print("Banco de dados inexistente")
            elif er.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print(f"Usuário e/ou senha de conexão incorretos. {er.msg}")
            else:
                print(f"Error: {er.msg}")

    def insert_transcription(self, file_id, user_id, name, transcription_text,transcription_detailed, model):
        try:
            created_date = datetime.now().date()

            conn = self.bd_connection()
            #cursor = self.connection.execute('set max_allowed_packet=67108864')
            #cursor = self.cursor
            cursor = conn.cursor(buffered=True)
            sql = "INSERT INTO transcriptions (file_id, user_id, text, text_detailed, type, created_at) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (file_id, user_id, transcription_text, transcription_detailed, model, created_date)
            cursor.execute(sql, values)
            print("\n")
            print(cursor.rowcount, " registros inseridos.")
            print("\n")
            conn.commit()
            conn.close()

        except Error as er:
            print(f"Erro ao inserir transcrição (BD): {er}")

        except Exception as er:
            print(f"Erro ao inserir transcricao: {er}")

    def select_file_by_status(self, status):
        try:
            cursor = self.cursor
            #sql = "SELECT file_id,filename, user_id, file_path, upload_date FROM files WHERE file_id = ?"
            #sql = "SELECT id as file_id, user_id, display_name, file_path, file_name, status, created_at, updated_at FROM files WHERE id = %s"
            #sql += "LIMIT 2"
            #cursor.execute(sql)
            #sql = "SELECT * FROM files f WHERE status = {} LIMIT 2".format(status)
            sql = "SELECT id, user_id, name, display_name, file_path, file_name, status, type, transcription_start, transcription_end, created_at, updated_at FROM files f WHERE status = {} LIMIT 2".format(status)

            cursor.execute(sql)

            #cursor.execute(f"SELECT id as file_id, user_id, display_name, file_path, file_name, status, created_at, updated_at,  FROM files WHERE status =1")

            dados = cursor.fetchall()
            return dados

        except Exception as er:
            print(f"Error: {er}")
            return []
    def update_file_status(self, file_id, status):
        try:
            date_now = datetime.now().date()

            conn = self.bd_connection()
            cursor = conn.cursor(buffered=True)
            '''sql = "UDPATE files f  "
            sql += " set status like {}".format(status)
            sql += "WHERE id = {}".format(file_id) 
            cursor.execute(sql)'''
            if status == 3:
                sql = "UPDATE files SET status = %s, transcription_end=%s  WHERE id = %s"
            else:
                sql = "UPDATE files SET status = %s, transcription_start=%s  WHERE id = %s"

            values = (status, date_now, file_id)
            cursor.execute(sql, values)
            cursor = self.cursor
            print(cursor.rowcount, " registros atualizados.\n")
            conn.commit()
            conn.close()
            return True
        except Exception as er:
            print(f"Erro ao realizar atualização na tabela files: {er}")
            return False


def transcribe_file(filepath: str, model_type="base", out="default", language='pt'):

    model = whisper.load_model(model_type)
    result = model.transcribe(filepath)
    ret = ""
    lista_text = []  # id, time_inicio, time_fim, texto
    for seg in result['segments']:
        td_s = timedelta(milliseconds=seg["start"] * 1000)
        td_e = timedelta(milliseconds=seg["end"] * 1000)

        t_s = f'{td_s.seconds // 3600:02}:{(td_s.seconds // 60) % 60:02}:{td_s.seconds % 60:02}.{td_s.microseconds // 1000:03}'
        t_e = f'{td_e.seconds // 3600:02}:{(td_e.seconds // 60) % 60:02}:{td_e.seconds % 60:02}.{td_e.microseconds // 1000:03}'

        ret += '{}\n{} --> {}\n{}|\n\n'.format(seg["id"], t_s, t_e, seg["text"])
        #ret += '{}\n'.format(seg["text"])
        #lista_text.append([seg["id"], t_s, t_e, seg["text"]])

        # -----
        #return lista_text
    return {"text": result['text'], "text_detailed": ret}


'''def download_file(filepath: str):
    with open(filepath, 'r') as file:
        filedata = file.read()
        '''


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    conexao_db = Connect()

    files = conexao_db.select_file_by_status(status=1)

    for file in files:
        print(file)
        file_id = file[0]
        user_id = file[1]
        name = file[2]

        full_filepath = file[4]+'/'+file[5]

        print("full_filepath: ", full_filepath)
        model = 'base'
        if file[7] == '1':
            model = 'tyny'
        elif file[7] == '2':
            model = 'base'
        elif file[7] == '3':
            model = 'small'
        elif file[7] == '4':
            model = 'medium'
        elif file[7] == '5':
            model = 'large'

        print('Iniciando transcrição....')
        if conexao_db.update_file_status(file_id, 2):
            result = transcribe_file(filepath=full_filepath, model_type=model)
            print('Inserindo transcrição na base de dados')
            conexao_db.insert_transcription(file_id, user_id, name, result['text'], result['text_detailed'], model)
            print('Transcrição do inserida')
            print(f'file_id: {file_id}')
            print(f'user_id: {user_id}')
            print(f'name: {name}')
            print(f'full_filepath: {full_filepath}')
            if conexao_db.update_file_status(file_id, 3):
                print('Processo finalizado')
            else:
                print("A transcrição foi inserida, mas não foi possível atualizar o status da tabela files.")
        else:
            print("Não foi possível iniciar a transcrição")



