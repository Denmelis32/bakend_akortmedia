import os
import sys
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Проверка наличия пакета
try:
    import ydb
    from ydb.iam import ServiceAccountCredentials
    print(f'✅ Пакет ydb найден')
except ImportError:
    print('❌ Пакет ydb не найден, устанавливаем...')
    os.system('pip install ydb[yc] --quiet')
    import ydb
    from ydb.iam import ServiceAccountCredentials

# Конфигурация
YDB_ENDPOINT = os.getenv('YDB_ENDPOINT')
YDB_DATABASE = os.getenv('YDB_DATABASE')
SA_KEY_PATH = os.getenv('YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS', '/workspace/auth-service/sa-key.json')

print(f'🔌 Подключение к YDB: {YDB_ENDPOINT}')
print(f'📂 База: {YDB_DATABASE}')
print(f'🔑 Ключ SA: {SA_KEY_PATH}')

# Читаем ключ сервисного аккаунта из файла
import json
with open(SA_KEY_PATH, 'r') as f:
    sa_key = json.load(f)

service_account_id = sa_key.get('service_account_id')
access_key_id = sa_key.get('id')
private_key = sa_key.get('private_key')

# Инициализация драйвера с сервисным аккаунтом
credentials = ServiceAccountCredentials(service_account_id, access_key_id, private_key)
driver_config = ydb.DriverConfig(
    endpoint=YDB_ENDPOINT,
    database=YDB_DATABASE,
    credentials=credentials
)
driver = ydb.Driver(driver_config)

try:
    driver.wait(timeout=10)
    print('✅ Соединение с YDB установлено')
    
    # Получаем пул сессий
    pool = ydb.SessionPool(driver)
    
    # Проверяем таблицу messages
    print('\n📩 Проверяем таблицу messages...')
    query = 'SELECT * FROM messages ORDER BY created_at DESC LIMIT 5;'
    
    def run_query(session):
        result = session.transaction().execute(
            query,
            commit_tx=True
        )
        return result

    results = pool.retry_operation_sync(run_query)
    
    if not results[0].rows:
        print('⚠️ Таблица messages пуста.')
    else:
        print(f'Найдено сообщений: {len(results[0].rows)}')
        for row in results[0].rows:
            print('-' * 40)
            row_dict = {col.name: row[col.name] for col in results[0].columns}
            print(f'ID: {row_dict.get("id", "N/A")}')
            print(f'Text: {str(row_dict.get("text", ""))[:50]}...')
            att = row_dict.get("attachment_url") or row_dict.get("attachments") or "Нет вложений"
            print(f'Attachment: {att}')
            print(f'Sender: {row_dict.get("sender_id", "N/A")}')
            print(f'Time: {row_dict.get("created_at", "N/A")}')
            
except Exception as e:
    print(f'❌ Ошибка: {e}')
    # Пробуем другие возможные имена таблиц
    possible_tables = ['messages', 'chat_messages', 'message', 'posts']
    for tbl in possible_tables:
        try:
            test_query = f'SELECT * FROM {tbl} LIMIT 1;'
            def test_q(session):
                return session.transaction().execute(test_query, commit_tx=True)
            pool.retry_operation_sync(test_q)
            print(f'✅ Таблица найдена: {tbl}')
            break
        except:
            print(f'❌ Таблица {tbl} не найдена')
    import traceback
    traceback.print_exc()
finally:
    driver.stop()
