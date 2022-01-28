from typing import ValuesView
import schedule
import telepot
import time
import pandas as pd
import numpy as np
import requests

from rt.rt_italy import generate_rt_csv, get_all_data,plot_rt
from covid_stat import plot_cases
from covid.data_it import get_and_process_covid_data_it

idx = pd.IndexSlice

def compute_and_send(data: pd.DataFrame):
    plot_rt()
    plot_cases()
    #Telegram
    # Se vuoi mandare messaggi solo al bot usa chat_id=405229696, altrimenti al gruppo chat_id=-1001578600515
    #bot.sendDocument(405229696, document=open('Covid/casi.pdf', 'rb'))
    bot = telepot.Bot('5023870649:AAGSGZaOQMzkGx43o1G0yP888-iDN-vzut0')


    bot.sendMessage(-1001578600515, "Andamento Positivi: ")
    bot.sendPhoto(-1001578600515, photo=open('/home/fabio/CovidAnalysisBot/Grafici/casi.png', 'rb'))

    bot.sendMessage(-1001578600515, "Andamento Rt: ")
    bot.sendPhoto(-1001578600515, photo=open('/home/fabio/CovidAnalysisBot/Grafici/Rt.png', 'rb'))

    #new_cases = cases[len(cases)-1];
    bot.sendMessage(-1001578600515, "Nuovi casi di Covid-19: "        + str(data.loc[idx["Italia"], "positive"][-1]) + "\n"
                                   "Numero Tamponi Effettuati: " + str(data.loc[idx["Italia"], "total"][-1]) + "\n"
                                   "Deceduti: "                  + str(data.loc[idx["Italia"], "dead"][-1]) + "\n")

#schedule.every().day.at("18:01").do(compute_and_send)

import datetime

i=0
while True:
    data=get_and_process_covid_data_it(datetime.date.today())
    days= data.loc[idx["Italia"]].index

    if days[-1] == datetime.date.today():
        if(i==0):
            compute_and_send(data)
            i=1
    if days[-1] != datetime.date.today():
        i=0

    schedule.run_pending()
    time.sleep(200)#3 circa minuti