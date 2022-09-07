# from msilib.schema import Error
import pandas as pd
import dvc.api
import sys 
import os

sys.path.append(os.path.abspath(os.path.join("./scripts/")))

class ReadWriteDVC():
    def dvc_get_data(self, path, version='v1') :
        try:
            repo = "~/timeseries_PharmaceuticalSalesPrediction"
            data_url = dvc.api.get_url(path=path, repo=repo, rev=version)
            data_url = str(data_url)[6:]
            df = pd.read_csv(data_url, sep=",", low_memory=False)
        except Exception as e:
            df = None
        return df
    
    def to_csv(self, df, csv_path, index=False):
        try:
            df.to_csv(csv_path, index=index)
        except Exception as e:
            # raise Error("did not write to csv file")
            pass