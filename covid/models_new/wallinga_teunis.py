import os
import glob
import pandas as pd
from rpy2 import robjects
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from covid.models_new.rt_method import rt_method

class wallinga_teunis(rt_method):
    def __init__(self, data, regions, name):
        super().__init__(data, regions, name)        
        
    def run_model(self, region_name):
        # Get data about region
        cases = self.data.loc[region_name]
        # Generate unique name for temporary file
        temp_file = 'temp/temp_{0}.txt'.format(abs(hash(region_name)) % (10 ** 8))
        # Write cases to txt temporary file
        cases['positive'].astype(int).to_csv(temp_file, header=False, index=False)
        R_code = """
            library(EpiEstim)
            mean_si <- 6.6
            std_si <- 4.88
            n_sim <- 10
            covid <- read.table("%s")
            res <- wallinga_teunis(covid,
                method = "parametric_si",
                config = list(
                    t_start = seq(2, %s),
                    t_end = seq(8, %s),
                    mean_si = mean_si,
                    std_si = std_si,
                    n_sim = n_sim
                ))
            res
        """ % (temp_file, len(cases)-6, len(cases))
        # Execute the R code
        result = robjects.r(R_code)
        print(self.name, region_name)
        return self.name, region_name, result
    def init_run(self):
        self.all_output = pd.DataFrame(columns=['region','date','mean','Low_975','High_975'])
        location = self.data.index.get_level_values(0).unique()[0]
        self.cases = self.data.loc[location]
    def step_run(self, r):
        with localconverter(ro.default_converter + pandas2ri.converter):
            final_result = ro.conversion.rpy2py(r[1][0])
            # Create dictionary to append
            d = {
                'region': r[0],
                'date': self.cases.index[len(self.cases)-len(final_result.index):],
                'mean': final_result['Mean(R)'],
                'std': final_result['Std(R)'],
                'Low_975': final_result['Quantile.0.025(R)'],
                'High_975': final_result['Quantile.0.975(R)']
            }
            # Append the results to output DataFrame
            self.all_output = self.all_output.append(pd.DataFrame(data=d), ignore_index=True)
    def end_run(self):
        temp_files = glob.glob('temp/*')
        for f in temp_files:
            os.remove(f)
