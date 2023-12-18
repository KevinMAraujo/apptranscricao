import schedule
import time
from main import Connect, transcribe_file

def task():
    f = open("task.txt", "a")
    f.write("Executando transcrição!\n")


    conexao_db = Connect()

    files = conexao_db.select_file_by_status(status='%')

    for file in files:
        print(file)
        file_id = file[0]
        user_id = file[1]
        name = file[2]

        full_filepath = file[3] + '/' + file[4]
        model = 'base'
        f.write("Iniciando transcrição....\n")
        print('Iniciando transcrição....')
        result = transcribe_file(filepath=full_filepath, model_type=model)
        f.write("Inserindo transcrição na base de dados\n")
        print('Inserindo transcrição na base de dados')
        conexao_db.insert_transcription(file_id, user_id, name, result, model)
        f.write("Transcrição do inserida\n")
        print('Transcrição do inserida')
        print(f'file_id: {file_id}')
        print(f'user_id: {user_id}')
        print(f'name: {name}')
        print(f'full_filepath: {full_filepath}')
    f.close()
schedule.every().minute.do(task) # Run every minute

while True:
    schedule.run_pending()
    time.sleep(1000)