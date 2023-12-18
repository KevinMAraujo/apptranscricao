from crontab import CronTab
import os

cron = CronTab(user=True)

path = os.path.abspath("./main.py")
#job = cron.new(command="python " + "'"+ path + "'")
job = cron.new(command="D:\Projetos\app de Transcricao_fastAPI\apptranscricao\app_venv\Scripts\python " + "'"+ path + "'")
job.minute.every(5)
job.set_comment("Finalizando")

cron.write()