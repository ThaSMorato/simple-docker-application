import psycopg2
import redis
import json
import os
from bottle import Bottle, request



SQL = "INSERT INTO emails (assunto, mensagem) VALUES (%s, %s)"

class Sender(Bottle):

    def __init__(self):
        super().__init__()
        self.route('/', method="POST", callback=self.send)

        redis_host = os.getenv('REDIS_HOST', 'queue')

        self.fila = redis.StrictRedis(host=redis_host, port=6379, db=0)

        db_host = os.getenv('DB_HOST', 'db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_name = os.getenv('DB_NAME', 'sender')
        db_password = os.getenv('DB_PASSWORD', 'admin')
        DSN = f"dbname={db_name} user={db_user} password={db_password} host={db_host}"
        print(DSN)
        self.conn = psycopg2.connect(DSN)

    def register_message(self, assunto, mensagem):
        
        cur = self.conn.cursor()
        cur.execute(SQL, (assunto, mensagem))
        self.conn.commit()
        cur.close()
        
        msg = {'assunto': assunto, 'mensagem': mensagem}
        self.fila.rpush('sender', json.dumps(msg))

        print('Mensagem registrada')


    def send(self):
        assunto = request.forms.get('assunto')
        message = request.forms.get('message')

        self.register_message(assunto, message)

        return f"Mensagem enfileirada ! Assunto {assunto} Mensagem {message}"

if __name__ == '__main__':
    sender = Sender()
    sender.run(host='0.0.0.0', port=8080, debug=True)