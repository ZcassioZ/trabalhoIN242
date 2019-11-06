import random
import time
import paho.mqtt.client as mqtt
import json
from pymongo import MongoClient
import pandas as pd

# Conectar ao broker

print('Conectando ao mqtt broker...')
mqtt_client = mqtt.Client()
mqtt_client.connect('3.132.161.196', 1883, 60)


#Produzir dados por uma hora
#variável tempo em minutos

tempo = 60
t_end = time.time() + 60*tempo

while time.time() < t_end:
    temperatura = random.uniform(15,30)
    print(temperatura)
    msg = {
        'temperatura': temperatura
    }
    mqtt_client.publish('in242', json.dumps(msg), qos=0)
    time.sleep(1)

#Conecta ao mongo
mongo_client = MongoClient('3.132.161.196', 27017)
mongo_db = mongo_client['inatel']
mongo_collection = mongo_db['in242']

# Cria arquivo CSV direto após a coleta
dados = list(mongo_collection.find())
df = pd.DataFrame(dados)
df.to_csv('analise_temp.csv')
print(df)
