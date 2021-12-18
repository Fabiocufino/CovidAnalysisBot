import pandas as pd
import math
from scipy.signal import savgol_filter
from covid.models_new.rt_method import rt_method

class rki(rt_method):
    def __init__(self, data, regions, name):
        super().__init__(data, regions, name)
        
    def run_model(self, region_name):
        cases = self.data.loc[region_name].reset_index()
        cases['smooth'] = savgol_filter(cases['positive'], 15, 3) # window size 7, polynomial order 3
        dd=[]
        for index, row in cases[9:].iterrows():
            DATE = row['date'].date()
            TODAY = row['smooth'] # cases today
            TODAY_9 = cases.loc[index-9]['smooth'] # cases 9 days ago
            TODAY_8 = cases.loc[index-8]['smooth'] # cases 8 days ago
            TODAY_7 = cases.loc[index-7]['smooth'] # cases 7 days ago
            TODAY_6 = cases.loc[index-6]['smooth'] # cases 6 days ago
            TODAY_5 = cases.loc[index-5]['smooth'] # cases 5 days ago
            TODAY_4 = cases.loc[index-4]['smooth'] # cases 4 days ago
            TODAY_3 = cases.loc[index-3]['smooth'] # cases 3 days ago
            TODAY_2 = cases.loc[index-2]['smooth'] # cases 2 days ago
            TODAY_1 = cases.loc[index-1]['smooth'] # cases 1 day ago
            S01 = TODAY + TODAY_1 + TODAY_2  + TODAY_3 +TODAY_4
            S02 =  TODAY_5 + TODAY_6 + TODAY_7 +TODAY_8 + TODAY_9 

            Rt = 0
            if(S02 > 0):
                Rt = S01 / S02

            Err = 0
            if(S01 > 0 and S02 > 0):
                Err = Rt * math.sqrt(1 / S01 + 1 / S02)

            High_Rt = Rt + Err

            Low_Rt = 0
            if (Rt >= Err):
                Low_Rt = Rt - Err

            dd.append({
                'region': region_name,
                'date': DATE,
                'mean': Rt,
                'Low_68': Low_Rt,
                'High_68': High_Rt,
                'Err': Err,
            })
                # Append the results to output DataFrame
        print(self.name, region_name)
        return self.name, region_name, dd;
    def init_run(self):
        self.all_output = pd.DataFrame(columns=['region','date','mean','Low_68','High_68','Err'])
    def step_run(self, r):
        for d in r[1]:
            self.all_output = self.all_output.append(d, ignore_index=True)
    def end_run(self):
        return
