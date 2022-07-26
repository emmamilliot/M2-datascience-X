#Importing modules
import json
import pandas as pd
from kafka import KafkaConsumer
from river import datasets
from river import evaluate
from river import neural_net as nn
from river import optim
from river import preprocessing as pp
from river import metrics
from river.stream import iter_pandas
from river.tree import HoeffdingTreeClassifier, HoeffdingTreeRegressor
from river.neighbors import KNNClassifier
from river.stream import iter_pandas
import numpy as np
import matplotlib.pyplot as plt

#Declearing consumer connection
try:
    consumer = KafkaConsumer('yfinanceapi',bootstrap_servers=['localhost:9092'])
except:
    print('connection error')
preds = open('pred_archive.txt','a')
trues = open('true_archive.txt','a')

#model
model=(pp.StandardScaler() |
       HoeffdingAdaptiveTreeRegressor(
           grace_period=100,
           leaf_prediction='adaptive',
           model_selector_decay=0.9)
        )

def construction_dataset(data_):
  X = data_.iloc[:-1,:]
  y = data_['Close'].iloc[1:]
  return list(iter_pandas(X=X, y=y))

def print_progress(sample_id, acc, MAE):
    print(f'Samples processed: {sample_id}')
    print(acc)
    print(MAE)

def stock_prediction(n_wait=3, verbose=False):
    acc = metrics.MSE()
    acc_rolling = metrics.Rolling(metric=metrics.MSE(), window_size=n_wait)
    MAE =  metrics.MAE()
    MAE_rolling = metrics.Rolling(metric=metrics.MAE(), window_size=n_wait)
    raw_results = []
    true_y = []
    pred_y=[]
    model_name = model.__class__.__name__
    y=0
    y_pred = None
    for i, msg in enumerate(consumer):
        data = json.loads(msg.value.decode('utf-8'))
        # Predict
        y_pred_prev = y_pred
        actual_value = data['Close']
        y = data['y_true']
        del data['y_true']
        x = data
        # print('x',x,'y',y)
        y_pred = model.predict_one(x)
        true_y.append(actual_value)
        pred_y.append(y_pred_prev)
        print(f'The predicted value was {y_pred_prev}, the actual value is {actual_value}')
        # plt.scatter(i,y_pred_prev)
        # plt.scatter(i,actual_value)

        if not y is None:
            # Update metrics and results
            acc=acc.update(y_true=y, y_pred=y_pred)
            acc_rolling=acc_rolling.update(y_true=y, y_pred=y_pred)
            MAE = MAE.update(y_true=y, y_pred=y_pred)
            MAE_rolling = MAE_rolling.update(y_true=y, y_pred=y_pred)
            if i % n_wait == 0 and i > 0:
                if verbose:
                    print_progress(i, acc_rolling, MAE_rolling)
                raw_results.append([model_name, i, acc.get(), acc_rolling.get(), MAE.get(), MAE_rolling.get()])
            # Learn (train)
            model.learn_one(x, y)
        
        np.save('y_true.npy', true_y)
        np.save('y_pred.npy', pred_y)

        preds.write(str(actual_value)+',')
        trues.write(str(y_pred_prev)+',')
    return pd.DataFrame(raw_results, columns=['model', 'id', 'MSE', 'MSE_roll', 'MAE', 'MAE_roll']), true_y, pred_y

# #getting data and predicting result using the model
# def stock_prediction():
#         # try:
#             y_pred_lst = []
#             y_true_lst = [0]
#             for msg in consumer:
#                 data = json.loads(msg.value.decode('utf-8'))
#                 print(data)



stock_prediction(verbose=True)
