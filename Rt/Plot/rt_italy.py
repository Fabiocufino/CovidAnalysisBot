import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

idx = pd.IndexSlice


def get_all_data():
    path= "Rt/Data/Rt_bettencourt_ribeiro.csv"
    all_data=pd.read_csv(path)
    all_data=all_data.set_index(["region","date"])
    return all_data

def select_region(all_data: pd.DataFrame, region):
    region_data=all_data.loc[idx[region]]
    return region_data


all_data=get_all_data()
region_data=select_region(all_data,"Italia")

fig,ax = plt.subplots()
days=region_data.index
val_rt=region_data["ML"]
ax.plot(days, val_rt,label="Indice Rt italia" )
plt.legend(loc="upper right")
plt.xlabel('Giorni')
plt.xticks(rotation=45)
plt.ylabel('R_t')
plt.grid(axis="y",linestyle='--', linewidth=1)



Low_95_val_rt=region_data["Low_95"]
High_95_val_rt=region_data["High_95"]
plt.fill_between(days,Low_95_val_rt, High_95_val_rt, color='green', alpha=0.2)

Low_90_val_rt=region_data["Low_90"]
High_90_val_rt=region_data["High_90"]
plt.fill_between(days,Low_90_val_rt, High_90_val_rt, color='red', alpha=0.2)

Low_50_val_rt=region_data["Low_50"]
High_50_val_rt=region_data["High_50"]
plt.fill_between(days,Low_50_val_rt, High_50_val_rt, color='yellow', alpha=0.2)

print(all_data)
plt.show()