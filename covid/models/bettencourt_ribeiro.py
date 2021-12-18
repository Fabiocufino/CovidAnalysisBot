import pandas as pd
import numpy as np
import concurrent.futures

from scipy import stats as sps
from scipy.interpolate import interp1d


class bettencourt_ribeiro():
    def __init__(self, data, regions):
        self.data=data
        self.regions=regions
        self.R_T_MAX = 12
        self.r_t_range = np.linspace(0, self.R_T_MAX, self.R_T_MAX*100+1)
        self.GAMMA = 1/6.6
        self.ROLLING_DAYS = 7
        

    def highest_density_interval(self, pmf, p=.9, debug=False):
        if(isinstance(pmf, pd.DataFrame)):
            return pd.DataFrame([self.highest_density_interval(pmf[col], p=p) for col in pmf],
                                index=pmf.columns)

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

        return pd.Series([low, high], index=[f'Low_{p*100:.0f}', f'High_{p*100:.0f}'])


    def prepare_cases(self, cases):
        new_cases = cases.diff()

        smoothed = new_cases.rolling(self.ROLLING_DAYS,
            win_type='gaussian',
            min_periods=1,
            center=True).mean(std=3).round()

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
        likelihoods = pd.DataFrame(
            data = sps.poisson.pmf(sr[1:].values, lam),
            index = self.r_t_range,
            columns = sr.index[1:])

        # (3) Create the Gaussian Matrix
        process_matrix = sps.norm(loc=self.r_t_range,
                                  scale=sigma
                                 ).pdf(self.r_t_range[:, None]) 

        # (3a) Normalize all rows to sum to 1
        process_matrix /= process_matrix.sum(axis=0)

        # (4) Calculate the initial prior
        prior0 = np.ones_like(self.r_t_range)/len(self.r_t_range)
        prior0 /= prior0.sum()

        # Create a DataFrame that will hold our posteriors for each day
        # Insert our prior as the first posterior.
        posteriors = pd.DataFrame(
            index=self.r_t_range,
            columns=sr.index,
            data={sr.index[0]: prior0}
        )

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
        
        print(region_name)

        return result, region_name
    
    
    def run(self):
        sigmas = np.linspace(1/20, 1, 20)
        results = {}
        
        with concurrent.futures.ProcessPoolExecutor() as executor:
            res = executor.map(self.run_model, self.regions)
            for r in list(res):
                # Store all results keyed off of state name
                results[r[1]] = r[0]

        # Each index of this array holds the total of the log likelihoods for
        # the corresponding index of the sigmas array.
        total_log_likelihoods = np.zeros_like(sigmas)

        # Loop through each state's results and add the log likelihoods to the running total.
        for region_name, result in results.items():
            total_log_likelihoods += result['log_likelihoods']

        # Select the index with the largest log likelihood total
        max_likelihood_index = total_log_likelihoods.argmax()

        final_results = None

        for region_name, result in results.items():
            posteriors = result['posteriors'][max_likelihood_index]
            hdis_95 = self.highest_density_interval(posteriors, p=.95)
            hdis_90 = self.highest_density_interval(posteriors, p=.90)
            hdis_68 = self.highest_density_interval(posteriors, p=.68)
            hdis_50 = self.highest_density_interval(posteriors, p=.50)
            most_likely = posteriors.idxmax().rename('ML')
            result = pd.concat([most_likely, hdis_95, hdis_90, hdis_68, hdis_50], axis=1)
            result.insert(0, 'region', region_name)
            result.insert(1, 'date', most_likely.index)
            if final_results is None:
                final_results = result
            else:
                final_results = pd.concat([final_results, result])
                
        final_results = final_results.groupby('region').apply(lambda x: x.iloc[1:])
        return final_results
    