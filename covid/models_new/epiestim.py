import os
import glob
import pandas as pd
from rpy2 import robjects
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from covid.models_new.rt_method import rt_method

class epiestim(rt_method):
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
            std_mean_si <- 1.88
            std_si <- 4.88
            std_std_si <- 1.03
            min_mean_si <- 1.90
            max_mean_si <- 11.30
            min_std_si <- 2.38
            max_std_si <- 7.38
            n1 <- 100
            n2 <- 100
            
            covid <- read.table("%s")
            res <- estimate_R(covid,
                method = "uncertain_si",
                config = make_config(list(
                    mean_si = mean_si,
                    std_mean_si = std_mean_si,
                    std_si = std_si,
                    std_std_si = std_std_si,
                    min_mean_si = min_mean_si,
                    max_mean_si = max_mean_si,
                    min_std_si = min_std_si,
                    max_std_si = max_std_si,
                    n1 = n1, n2 = n2,
                    t_start = seq(2, %s),
                    t_end = seq(8, %s)
                 )))
            res
        """ % (temp_file, len(cases)-6, len(cases))
        # Execute the R code
        result = robjects.r(R_code)
        print(self.name, region_name)
        return self.name, region_name, result
    def init_run(self):
        self.all_output = pd.DataFrame(columns=['region','date','mean','median','Low_95','High_95','Low_75','High_75'])
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
                'median': final_result['Median(R)'],
                'Low_95': final_result['Quantile.0.05(R)'],
                'High_95': final_result['Quantile.0.95(R)'],
                'Low_75': final_result['Quantile.0.25(R)'],
                'High_75': final_result['Quantile.0.75(R)']
            }
            # Append the results to output DataFrame
            self.all_output = self.all_output.append(pd.DataFrame(data=d), ignore_index=True)
    def end_run(self):
        temp_files = glob.glob('temp/*')
        for f in temp_files:
            os.remove(f)
