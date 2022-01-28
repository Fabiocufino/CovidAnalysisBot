import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

idx = pd.IndexSlice

import time
import pandas as pd
import numpy as np


def generate_rt_csv():
    # Dataset
    from covid.data import get_data
    # Models
    from covid.models.covidstat import covidstat

    data = get_data(country='it', run_date=pd.Timestamp.today())

    #regions = data.index.get_level_values(0).unique()
    regions = data.index.get_level_values(0).unique()[7]#Si prende solo l'Italia

    total_execution_time = time.time()

    start_time = time.time()

    cs = covidstat(data, regions)
    output_covidstat = cs.run_region(regions)
    output_covidstat.to_csv('/home/fabio/CovidAnalysisBot/Rt_covidstat.csv', index=False)



def get_all_data():
    path= "/home/fabio/CovidAnalysisBot/Rt_covidstat.csv"
    all_data=pd.read_csv(path)
    all_data = all_data.set_index(["data"]).sort_index()
    return all_data

def plot_rt():
    generate_rt_csv()
    all_data=get_all_data()

    fig,ax = plt.subplots()
    days=all_data.index
    val_rt=all_data["Rt"]
    ax.plot(days, val_rt,'-bo',markersize=2, label="Indice Rt italia",color="r" )
    ax.set_ylim(0.4,val_rt[-1]+1)

    plt.xlabel('Giorni')
    plt.ylabel('R_t')
    plt.grid(axis="y",linestyle='--', linewidth=1)
    plt.xticks(rotation=35)
    xtick_loc=[i for i in range(len(days)-1) if(i%15==0)]
    xtick_loc.append(len(days)-1)
    ax.set_xticks(xtick_loc)


    plt.xlim(len(days)-100,len(days)-1)

    Low_95_val_rt=all_data["Low_95"]
    High_95_val_rt=all_data["High_95"]
    plt.fill_between(days,Low_95_val_rt, High_95_val_rt, color='green', alpha=0.1,label="95% conf.")

    Low_68_val_rt=all_data["Low_68"]
    High_68_val_rt=all_data["High_68"]
    plt.fill_between(days,Low_68_val_rt, High_68_val_rt, color = "dodgerblue", alpha=0.2,label="68% conf.")

    plt.legend(loc="upper left")
    fig.set_size_inches(10.5, 7, forward=True)
    fig.tight_layout() #Serve er far veere tutta la igura senza assi tagliati
    plt.savefig('/home/fabio/CovidAnalysisBot/Grafici/Rt.png', dpi=399)
