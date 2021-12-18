import os
import glob
import pandas as pd
import concurrent.futures
from rpy2 import robjects
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter


class epiestim():
    def __init__(self, data, regions):
        self.data=data
        self.regions=regions
        
        
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
            mean_prior <- {mean_prior:.4f}
            std_prior <- {std_prior:.4f}
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
                    mean_prior = mean_prior,
                    std_prior = std_prior,
                    n1 = n1, n2 = n2,
                    t_start = seq(2, %s),
                    t_end = seq(8, %s)
                 )))
            res
        """ % (temp_file, len(cases)-6, len(cases))
        R_code = R_code.format(mean_prior=self.mean_prior, std_prior=self.std_prior)
        # Execute the R code
        result = robjects.r(R_code)
        print(region_name)
        return result, region_name
    
    
    def run(self, mean_prior=5, std_prior=5):
        self.mean_prior = mean_prior
        self.std_prior = std_prior
        # Create output DataFrame
        all_output = pd.DataFrame(columns=['region','date','mean','median','Low_95','High_95','Low_75','High_75'])
        # Only for indexing date
        location = self.data.index.get_level_values(0).unique()[0]
        cases = self.data.loc[location]
        # Execute the process pool
        with concurrent.futures.ProcessPoolExecutor() as executor:
            res = executor.map(self.run_model, self.regions)
            # Iterate the results
            for r in list(res):
                # Convert rpy2 DataFrame to Pandas Dataframe
                with localconverter(ro.default_converter + pandas2ri.converter):
                    final_result = ro.conversion.rpy2py(r[0][0])
                    # Create dictionary to append
                    d = {
                        'region': r[1],
                        'date': cases.index[len(cases)-len(final_result.index):],
                        'mean': final_result['Mean(R)'],
                        'std': final_result['Std(R)'],
                        'median': final_result['Median(R)'],
                        'Low_95': final_result['Quantile.0.05(R)'],
                        'High_95': final_result['Quantile.0.95(R)'],
                        'Low_75': final_result['Quantile.0.25(R)'],
                        'High_75': final_result['Quantile.0.75(R)']
                    }
                    # Append the results to output DataFrame
                    all_output = all_output.append(pd.DataFrame(data=d), ignore_index=True)
        
        # Clean the temp folder
        temp_files = glob.glob('temp/*')
        for f in temp_files:
            os.remove(f)
            
        return all_output
