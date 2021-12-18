import pandas as pd
import numpy as np
from scipy import stats as sps
from scipy.interpolate import interp1d
from covid.models_new.rt_method import rt_method
import concurrent.futures

class bettencourt_ribeiro(rt_method):
    def __init__(self, data, regions, name):
        super().__init__(data, regions, name)
        self.R_T_MAX = 12
        self.r_t_range = np.linspace(0, self.R_T_MAX, self.R_T_MAX*100+1)
        self.GAMMA = 1/6.6
        self.ROLLING_DAYS = 7

    def highest_density_interval(self, cfg, debug=False):
        pmf, p = cfg
        if(isinstance(pmf, pd.DataFrame)):
            return p, pd.DataFrame([self.highest_density_interval((pmf[col], p))[1] for col in pmf], index=pmf.columns)
        cumsum = np.cumsum(pmf.values)
        # N x N matrix of total probability mass for each low, high
        total_p = cumsum - cumsum[:, None]
        # Return all indices with total_p > p
        remove_nan = lambda x: np.nan_to_num(x)
        total_p = remove_nan(total_p)
        lows, highs = (total_p > p).nonzero()
        # Find the smallest range (highest density)
        low, high = 0, 0
        if(len(highs) > 0 and len(lows) > 0):
            best = (highs - lows).argmin()
            low = pmf.index[lows[best]]
            high = pmf.index[highs[best]]
        return p, pd.Series([low, high], index=[f'Low_{p*100:.0f}', f'High_{p*100:.0f}'])
    
    def prepare_cases(self, cases):
        new_cases = cases.diff()
        smoothed = new_cases.rolling(self.ROLLING_DAYS, win_type='gaussian', min_periods=1, center=True).mean(std=3).round()
        smoothed_tmp = smoothed
        for i in range(10, 0, -1):
            idx_start = np.searchsorted(smoothed, i)
            smoothed_tmp = smoothed.iloc[idx_start:]
            original = new_cases.loc[smoothed_tmp.index]
            if len(smoothed_tmp) > 0:
                break
        return original, smoothed_tmp
    
    def get_posteriors(self, sr, sigma=0.15):
        # (1) Calculate Lambda
        lam = sr[:-1].values * np.exp(self.GAMMA * (self.r_t_range[:, None] - 1))
        # (2) Calculate each day's likelihood
        likelihoods = pd.DataFrame(data = sps.poisson.pmf(sr[1:].values, lam), index = self.r_t_range, columns = sr.index[1:])
        # (3) Create the Gaussian Matrix
        process_matrix = sps.norm(loc=self.r_t_range, scale=sigma).pdf(self.r_t_range[:, None]) 
        # (3a) Normalize all rows to sum to 1
        process_matrix /= process_matrix.sum(axis=0)
        # (4) Calculate the initial prior
        prior0 = np.ones_like(self.r_t_range)/len(self.r_t_range)
        prior0 /= prior0.sum()
        # Create a DataFrame that will hold our posteriors for each day
        # Insert our prior as the first posterior.
        posteriors = pd.DataFrame(index=self.r_t_range, columns=sr.index, data={sr.index[0]: prior0})
        # Track of the sum of the log of the probability
        # of the data for maximum likelihood calculation.
        log_likelihood = 0.0
        # (5) Iteratively apply Bayes' rule
        for previous_day, current_day in zip(sr.index[:-1], sr.index[1:]):
            #(5a) Calculate the new prior
            current_prior = process_matrix @ posteriors[previous_day]
            #(5b) Calculate the numerator of Bayes' Rule: P(k|R_t)P(R_t)
            numerator = likelihoods[current_day] * current_prior
            #(5c) Calcluate the denominator of Bayes' Rule P(k)
            denominator = np.sum(numerator)
            denominator = 0.000001 if denominator <= 0 else denominator
            # Execute full Bayes' Rule
            posteriors[current_day] = numerator/denominator
            # Add to the running sum of log likelihoods
            log_likelihood += np.log(denominator)
        return posteriors, log_likelihood
        
    def run_model(self, region_name):
        sigmas = np.linspace(1/20, 1, 20)
        cases = self.data['all_cases'][region_name].astype(int)
        new, smoothed = self.prepare_cases(cases)
        if len(smoothed) == 0:
            new, smoothed = self.prepare_cases(cases)
        result = {}
        # Holds all posteriors with every given value of sigma
        result['posteriors'] = []
        # Holds the log likelihood across all k for each value of sigma
        result['log_likelihoods'] = []
        for sigma in sigmas:
            posteriors, log_likelihood = self.get_posteriors(smoothed, sigma=sigma)
            result['posteriors'].append(posteriors)
            result['log_likelihoods'].append(log_likelihood)
        print(self.name, region_name)
        return self.name, region_name, result
    
    def init_run(self):
        self.results = {}
        self.all_output = None
        
    def step_run(self, r):
        print("B&R, step_run", r[0])
        self.results[r[0]] = r[1]
        
    def compute_posteriors(self, cfg):
        region_name, max_likelihood_index = cfg
        result = self.results[region_name]
        posteriors = result['posteriors'][max_likelihood_index]

        hdis = {}
        p95, p68 = .95, .68
        cfgs = ((posteriors, p95), (posteriors, p68))
# *** parallel version (memory expansive) ***
#        with concurrent.futures.ProcessPoolExecutor() as executor:
#            res = executor.map(self.highest_density_interval, cfgs)
#            for r in list(res):
#                p, h = r
#                print("B&R", region_name, "{:.2%}".format(p))
#                hdis[p] = h
# *** serial version ***
        for p in (p95, p68):
            p, h = self.highest_density_interval((posteriors, p))
            print("B&R", region_name, "{:.2%} CL".format(p))
            hdis[p] = h

        most_likely = posteriors.idxmax().rename('ML')
        return region_name, hdis[p95], hdis[p68], most_likely
    
    def end_run(self):
        print("B&R, end run")
        sigmas = np.linspace(1/20, 1, 20)
        total_log_likelihoods = np.zeros_like(sigmas)
        for region_name, result in self.results.items():
            total_log_likelihoods += result['log_likelihoods']
        max_likelihood_index = total_log_likelihoods.argmax()        
        cfgs = [(region_name, max_likelihood_index) for region_name in self.results.keys()] 
# *** parallel version (memory expansive) ***
#        with concurrent.futures.ProcessPoolExecutor() as executor:
#            res = executor.map(self.compute_posteriors, cfgs)
#            for r in list(res):
#                region_name, hdis_95, hdis_68, most_likely = r
#                posteriors = self.results[region_name]['posteriors'][max_likelihood_index]
#                result_cat = pd.concat([most_likely, hdis_95, hdis_68], axis=1)
#                result_cat.insert(0, 'region', region_name)
#                result_cat.insert(1, 'date', most_likely.index)
#                if self.all_output is None: self.all_output = result_cat
#                else: self.all_output = pd.concat([self.all_output, result_cat])
# *** serial version ***
        for region_name in self.results.keys():
            print("B&R, region: ", region_name)
            region_name, hdis_95, hdis_68, most_likely = self.compute_posteriors([region_name, max_likelihood_index])
            posteriors = result['posteriors'][max_likelihood_index]
            result_cat = pd.concat([most_likely, hdis_95, hdis_68], axis=1)
            result_cat.insert(0, 'region', region_name)
            result_cat.insert(1, 'date', most_likely.index)
            if self.all_output is None: self.all_output = result_cat
            else: self.all_output = pd.concat([self.all_output, result_cat])
                
        print("B&R, apply lambda")
        self.all_output = self.all_output.groupby('region').apply(lambda x: x.iloc[1:])        
        print("B&R, done")
    