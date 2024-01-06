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

    def insert_transcription(self, file_id, user_id, name, transcription_text, model):
        try:
            created_date = datetime.now().date()

            conn = self.bd_connection()
            #cursor = self.connection.execute('set max_allowed_packet=67108864')
            #cursor = self.cursor
            cursor = conn.cursor(buffered=True)
            sql = "INSERT INTO transcriptions (file_id, user_id, text, type, created_at) VALUES (%s, %s, %s, %s, %s)"
            values = (file_id, user_id, transcription_text, model, created_date)
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
            sql = "SELECT * FROM files f  "
            sql += "WHERE NOT EXISTS (SELECT t.file_id	FROM transcriptions t WHERE t.file_id = f.id LIMIT 1)"
            sql += "LIMIT 2"

            print('Aqui')
            cursor.execute(sql)
            #cursor.execute(f"SELECT id as file_id, user_id, display_name, file_path, file_name, status, created_at, updated_at FROM files WHERE status =1")

            file = cursor.fetchall()
            return file

        except Exception as er:
            print(f"Error: {er}")
            return None

def transcribe_file(filepath: str, model_type="base", out="default", language='pt'):

    model = whisper.load_model(model_type)

    result = model.transcribe(filepath)
    if out == "default":
        # ----
        ret = ""
        lista_text = []  # id, time_inicio, time_fim, texto
        for seg in result['segments']:
            td_s = timedelta(milliseconds=seg["start"] * 1000)
            td_e = timedelta(milliseconds=seg["end"] * 1000)

            t_s = f'{td_s.seconds // 3600:02}:{(td_s.seconds // 60) % 60:02}:{td_s.seconds % 60:02}.{td_s.microseconds // 1000:03}'
            t_e = f'{td_e.seconds // 3600:02}:{(td_e.seconds // 60) % 60:02}:{td_e.seconds % 60:02}.{td_e.microseconds // 1000:03}'

            #ret += '{}\n{} --> {}\n{}|\n\n'.format(seg["id"], t_s, t_e, seg["text"])
            ret += '{}\n'.format(seg["text"])
            #lista_text.append([seg["id"], t_s, t_e, seg["text"]])
        #ret += '\n'
        return {"text": ret}
        # -----
        #return lista_text
    elif out == "text":
        return {"text": result['text']}
        #return [result['text']]
    else:
        '''max_end = '00:00:00'
        if len(result['segments']) > 0:
            len_seg = len(result['segments']) - 1
            max_end = result['segments'][len_seg]['end']
            min_end = result['segments'][0]['start']'''
        return {"text": result['text']}
        #return [result['text']]

'''def download_file(filepath: str):
    with open(filepath, 'r') as file:
        filedata = file.read()
        '''


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    conexao_db = Connect()

    files = conexao_db.select_file_by_status(status='%')

    for file in files:
        print(file)
        file_id = file[0]
        user_id = file[1]
        name = file[2]


        full_filepath = file[4]+'/'+file[5]
        print("full_filepath: ", full_filepath)
        model = 'base'
        print('Iniciando transcrição....')
        result = transcribe_file(filepath=full_filepath, model_type=model)
        print('Inserindo transcrição na base de dados')
        #print("Result: ", result['text'])
        conexao_db.insert_transcription(file_id, user_id, name, result['text'], model)
        print('Transcrição do inserida')
        print(f'file_id: {file_id}')
        print(f'user_id: {user_id}')
        print(f'name: {name}')
        print(f'full_filepath: {full_filepath}')



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
