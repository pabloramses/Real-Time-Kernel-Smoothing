from flask import Flask,Response
import time
import json
import threading
import pandas as pd
import numpy as np
from KERNELREG import L1, RegKerVc, RegKerSc
from future_time import future_timestamp
import mysql.connector as mysql
import pmdarima as pmd
import statsmodels.api as sm


#DATABASE FOR PREDICTIONS
conn = mysql.connect(user='root',
   password='root',
   host='mariadb',
   port=3306,
   database='prediction')

#DELETE DATA FROM PREVIOUS TRIALS
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS prediction")
#create table
cursor = conn.cursor()
cursor.execute("CREATE TABLE prediction (time timestamp primary key, value float)")

app = Flask(__name__)
app2 = Flask("otro")

metric = 'hola'

prediccion = '60'

@app.route('/metrics')
def metrics():
    prometheus_line = ""
    for var in metric:
        prometheus_line += "# HELP " + var + "\n"
        prometheus_line += "# TYPE " + var + " gauge\n"
        prometheus_line += var + " " + str(metric[var]) + "\n"

    return Response(prometheus_line,mimetype='text/plain')

@app2.route('/predict')
def predict():
    prometheus_line = ""
    prometheus_line += "# HELP predict\n"
    prometheus_line += "# TYPE predict gauge\n"
    prometheus_line += "predict " + str(prediccion) + "\n"

    return Response(prometheus_line,mimetype='text/plain')

def callback(new_metric):
    global metric
    metric = new_metric
    p.new_element(metric["OutsideAirTemp"])


def run_service():
    threading.start_new_thread(flask_thread(),())


class RoundRobinList():
    lista = []
    def __init__(self,tamano):
        self.tamano = tamano

    def len(self):
        return len(self.lista)

    def push(self,elemento):
        if (len(self.lista) < self.tamano):
            self.lista.append(elemento)
        else:
            self.lista = self.lista[1:]
            self.lista.append(elemento)
    def print_lista(self):
        print(str(self.lista) + "\r")

    def to_array(self):
        return pd.to_numeric(np.array(self.lista))


class PredictThread(threading.Thread):

    def __init__(self,callback, smooth):
        threading.Thread.__init__(self)
        self.callback = callback
        self.smooth = smooth
    def run(self):
        time.sleep(2)
        self.callback(self.smooth)



class Predict(threading.Thread):

    prediciendo = False

    prediction_thread = None

    def __init__(self,tamano):
        threading.Thread.__init__(self)
        self.tamano = tamano
        self.lista = RoundRobinList(self.tamano)


    def new_element(self,elemento):
        self.lista.push(elemento)
        self.lista.print_lista()

    def callback_prediccion(self,resultado):
        self.prediciendo = False
        global prediccion
        prediccion = resultado
        print("ya tengo el resultado:  " , resultado)

    def predict(self):
        #procesar la lista y hacer prediccion
        if self.prediction_thread is None or not self.prediciendo:
            print("Me pongo a predecir")
            self.prediciendo = True
            y = self.lista.to_array()
            x = np.arange(0,self.tamano,1)
            #SMOOTHING
            Smooth_window = RegKerVc(x,x,y,L1,self.tamano)
            self.smooth = Smooth_window[-1]
            #PREDICTING
            selector = pmd.arima.auto_arima(Smooth_window,start_p=1, d=None, start_q=1, max_p=5, max_d=2, max_q=5, start_P=1, D=None, start_Q=1, max_P=2, max_D=1, max_Q=2, max_order=5, m=1, seasonal=True, stationary=False)
            model = sm.tsa.arima.ARIMA(Smooth_window, order = selector.order)
            fit = model.fit()
            prediction = fit.forecast(30)
            print('Tengo la prediccion')
            #TIMESTAMPS FOR FUTURE TIMES
            pred_time = future_timestamp(30)
            conn = mysql.connect(user='root',
   			password='root',
			host='mariadb',
   			port=3306,
   			database='prediction')
            cursor = conn.cursor()
            sql_insert = "REPLACE INTO prediction (time,value) values (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s), (%s,%s)"
            cursor.execute(sql_insert,(pred_time[0],float(prediction[0]),pred_time[1],float(prediction[1]),
              pred_time[2],float(prediction[2]),pred_time[3],float(prediction[3]),pred_time[4],
              float(prediction[4]),pred_time[5],float(prediction[5]),pred_time[6],float(prediction[6]),
              pred_time[7],float(prediction[7]),pred_time[8],float(prediction[8]),pred_time[9],
              float(prediction[9]),pred_time[10],float(prediction[10]),pred_time[11],float(prediction[11]),
              pred_time[12],float(prediction[12]),pred_time[13],float(prediction[13]),pred_time[14],
              float(prediction[14]),pred_time[15],float(prediction[15]),pred_time[16],float(prediction[16]),
              pred_time[17],float(prediction[17]),pred_time[18],float(prediction[18]),pred_time[19],
              float(prediction[19]),pred_time[20],float(prediction[20]),pred_time[21],float(prediction[21]),
              pred_time[22],float(prediction[22]),pred_time[23],float(prediction[23]),pred_time[24],
              float(prediction[24]),pred_time[25],float(prediction[25]),pred_time[26],float(prediction[26]),
              pred_time[27],float(prediction[27]),pred_time[28],float(prediction[28]),pred_time[29],
              float(prediction[29])))
            conn.commit()
            print('Acabo de añadir la prediccion a la base de datos')
            self.callback_prediccion(self.smooth)
            self.prediction_thread = PredictThread(self.callback_prediccion, self.smooth)
            self.prediction_thread.start()
        else:
            pass
            print("esta prediciendo")

    def run(self):
        while True:
            time.sleep(1)
            print("hilo, a dormir")
            if (self.lista.len() == self.tamano):
               print("ahora")
               self.predict()
            else:
               print("aun no")
               print("len: " + str(self.lista.len()) + "," + str(self.tamano))

p = Predict(60)

class PushMetrics(threading.Thread):

    def __init__(self,callback):
        threading.Thread.__init__(self)
        self.callback = callback

    def run(self):
        with open('mqtt_fixed.json','r') as f:
            while True:
                metric = f.readline()
                if metric:
                    metric_json = json.loads(metric)
                    self.callback(metric_json)
                    time.sleep(1)
                else:
                    break

class FlaskHilo(threading.Thread):
    def __init__(self,app,port):
        self.app = app
        self.port = port
        threading.Thread.__init__(self)

    def run(self):
        self.app.run(host='0.0.0.0',port=self.port)

if __name__ == '__main__':
    push = PushMetrics(callback)
    push.start()
    p.start()
    app1 = FlaskHilo(app,8000)
    app2 = FlaskHilo(app2,8001)
    app1.start()
    app2.start()
    #app.run(host='0.0.0.0',port=8000)
    #app.run(host='0.0.0.0',port=8001)
