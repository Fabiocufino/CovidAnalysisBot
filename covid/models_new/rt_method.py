import concurrent.futures

class rt_method(object):
    def __init__(self, data, regions, name):
        self.data=data
        self.regions=regions
        self.name=name
    def run_model(self, region_name):
        pass
    def init_run(self):
        pass
    def step_run(self,r):
        pass
    def end_run(self):
        pass
    def save(self):
        self.all_output.to_csv('Rt/Rt_'+self.name+'.csv', index=False)
#
# old partial parallelization, now obsolete
#
#    def run(self):
#        with concurrent.futures.ProcessPoolExecutor() as executor:
#            res = executor.map(self.run_model, self.regions)
#            for r in list(res): self.step_run(r)
