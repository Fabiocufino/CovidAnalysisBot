import time
import pandas as pd
import numpy as np

# Dataset
from covid.data import get_data
# Models
from covid.models.bettencourt_ribeiro import bettencourt_ribeiro

data = get_data(country='it', run_date=pd.Timestamp.today()) #Non funzionas un cazzo

regions = data.index.get_level_values(0).unique()

total_execution_time = time.time()

start_time = time.time()

br = bettencourt_ribeiro(data, regions)
output_bettencourt_ribeiro = br.run()
output_bettencourt_ribeiro.to_csv('Rt/Rt_bettencourt_ribeiro.csv', index=False)

print("\n- Executed in: %.2f seconds" % (time.time() - start_time))