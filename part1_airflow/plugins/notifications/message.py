from airflow.providers.telegram.hooks.telegram import TelegramHook 
import os
import dotenv

dotenv.load_dotenv()
TOKEN = os.environ.get("TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram_failure_message(context): # на вход принимаем словарь с контекстными переменными
    hook = TelegramHook(token=TOKEN, chat_id=CHAT_ID)
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG  c id={run_id} прошло неуспешно! задача {task_instance_key_str}' # определение текста сообщения
    hook.send_message({
        'chat_id': CHAT_ID,
        'text': message
    }) 

def send_telegram_success_message(context): # на вход принимаем словарь с контекстными переменными
    hook = TelegramHook(token=TOKEN, chat_id=CHAT_ID)
    dag = context['dag'].dag_id
    run_id = context['run_id']
    print("success")
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'text': message
    }) 