import pandas as pd
import numpy as np
import scipy.stats
import concurrent.futures

class covidstat():
    def __init__(self, data, regions):
        self.data=data
        self.regions=regions
        self.SHAPE = 1.87
        self.SCALE = 3.57
        self.INTERVAL = 14
        self.NDAYS_COV = 6.6
        self.NDAYS_COV_ERR = 1.88
        
    def run_region(self, region):
        start_date = self.data.loc['Italia'].index[:1][0]
        end_date = self.data.loc['Italia'].index[-1:][0]
        daily_cases = self.data.loc[region]['positive'].values
        
        x = list(range(self.INTERVAL))

        y = np.cumsum(daily_cases)
        y = np.clip(y - np.concatenate((np.full(1, np.nan), y[:-1])), 0, None)
        y = np.convolve(y, np.ones((self.INTERVAL,))/self.INTERVAL, mode='valid')
        y = np.concatenate((np.full(self.INTERVAL - 1, np.nan), y))
        y[y == 0] = np.nan
        y = np.log(y)
        
        dat = np.full(len(daily_cases), np.nan)
        err = np.full(len(daily_cases), np.nan)
        
        for i in range(self.INTERVAL, len(daily_cases)):
            dat[i], _, _, _, err[i] = scipy.stats.linregress(x, y[i-self.INTERVAL:i])
            
        rt_covidstat = lambda x: (1.0 + x * self.SCALE) ** self.SHAPE
        
        rt = rt_covidstat(dat)
        rt_err = rt * np.sqrt(((dat * self.NDAYS_COV_ERR)**2 + (err * self.NDAYS_COV)**2))
        rt_ci_high_68 = rt + rt_err + (((rt_err / rt)**2) * rt / 2)
        rt_ci_low_68  = rt - rt_err + (((rt_err / rt)**2) * rt / 2)
        rt_ci_high_95 = rt + (2 * rt_err) + (2 * ((rt_err / rt)**2) * rt)
        rt_ci_low_95  = rt - (2 * rt_err) + (2 * ((rt_err / rt)**2) * rt)
        
        df = pd.DataFrame(
            data={
                'data':    pd.date_range(start=start_date, end=end_date),
                'Rt':      rt,
                'High_68': rt_ci_high_68,
                'Low_68':  rt_ci_low_68,
                'High_95': rt_ci_high_95,
                'Low_95':  rt_ci_low_95
            }
        )
        print(region)
        return df;
            
    def run(self):
        # Create output DataFrame
        all_output = pd.DataFrame(columns=['data','Rt','High_68','Low_68','High_95','Low_95'])
        with concurrent.futures.ProcessPoolExecutor() as executor:
            res = executor.map(self.run_region, self.regions)
            for dd in list(res):
                all_output = all_output.append(dd, ignore_index=True)
        return all_output
