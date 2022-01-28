import time
import pandas as pd
import numpy as np

# Dataset
from covid.data import get_data
# Models
from covid.models.covidstat import covidstat

data = get_data(country='it', run_date=pd.Timestamp.today())

regions = data.index.get_level_values(0).unique()
#regions = [data.index.get_level_values(0).unique()[0],data.index.get_level_values(0).unique()[1]]#Si prende solo i primi due dell'index, Abruzzo e Basilicata

total_execution_time = time.time()

start_time = time.time()

cs = covidstat(data, regions)
output_covidstat = cs.run()
output_covidstat.to_csv('Rt/Rt_covidstat.csv', index=False)

print("\n- Executed in: %.2f seconds" % (time.time() - start_time))