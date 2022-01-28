import time
from covid.data_it import get_and_process_covid_data_it
import schedule
import telepot
import csv
import matplotlib.pyplot as plt
import numpy as np
import requests
import pandas as pd

# Dataset
from covid.data import get_data

from covid.data_it import (
    get_and_process_covid_data_it,
    get_raw_covid_data_it,
    process_covid_data_it,
)
idx = pd.IndexSlice
    
def plot_cases():
    #(START) GET DATA FOR CASES GRAPH
    data=get_and_process_covid_data_it(pd.Timestamp.today())

    days= data.loc[idx["Italia"]].index
    cases = data.loc[idx["Italia"], "positive"]
    dead=data.loc[idx["Italia"], "dead"] 
    #(END)


    #(START) GENERETE CASES GRAPH
    fig, ax1 = plt.subplots(2, 1)
    ax1[0].plot(days, cases,'o', markersize=1, label="Casi Covid Italia")
    ax1[0].set_ylabel('Daily Cases')
    ax1[0].grid(axis="y",linestyle='--', linewidth=1) 
    ax1[0].legend(["Casi Covid Italia"],loc="upper left")
    ax1[0].axes.get_xaxis().set_visible(False)


    ax1[1].plot(days, dead,'o', markersize=1, label="Casi Covid Italia", color="red")
    ax1[1].set_ylabel('Daily Dead')
    ax1[1].grid(axis="y",linestyle='--', linewidth=1)
    ax1[1].legend(["Morti Covid Italia"],loc="upper right")



    plt.xticks(rotation=45)
    fig.set_size_inches(10.5, 7, forward=True)
    fig.tight_layout() #Serve er far veere tutta la igura senza assi tagliati
    plt.savefig('/home/fabio/CovidAnalysisBot/Grafici/casi.png', dpi=399)
    #(END)




