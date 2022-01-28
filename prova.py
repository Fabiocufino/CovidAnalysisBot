import time
from covid.data_it import get_and_process_covid_data_it
import schedule
import requests
import telepot
import csv
import matplotlib.pyplot as plt
import numpy as np
import requests
import pandas as pd
import time
import pandas as pd
import numpy as np

# Dataset
from covid.data import get_data

from covid.data_it import (
    get_and_process_covid_data_it,
    get_raw_covid_data_it,
    process_covid_data_it,
)


idx = pd.IndexSlice

data=get_and_process_covid_data_it(pd.Timestamp.today())

days= data.loc[idx["Italia"]].index
cases = data.loc[idx["Italia"], "positive"]
tested = data.loc[idx["Italia"], "total"]

diff = cases/tested 

fig,ax1 = plt.subplots()
ax1.plot(days, diff,'o', markersize=1, label="Casi Covid Italia")
plt.legend(loc="upper left")
plt.xlabel('Giorni')
plt.xticks(rotation=45)
plt.ylabel('Casi Giornalieri')
plt.grid(axis="y",linestyle='-', linewidth=1)

fig.tight_layout() #Serve er far veere tutta la igura senza assi tagliati
plt.show()





