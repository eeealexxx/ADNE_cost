import os, requests, json, sys, urllib3, datetime
import pandas as pd
import multiprocessing as mp
import numpy as np

LOCAL_FILE_PATH = os.path.dirname(os.path.abspath(__file__))
GLOBAL_PATH = os.path.join(LOCAL_FILE_PATH,'..','..')
sys.path.insert(0, os.path.join(GLOBAL_PATH))

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from modules.modules_auxiliary import return_adaccount_dataframe
from modules.variables import Facebook_accounts_links


class Facebook_API_cost():

    def __init__(self, conf_file='facebook.conf'):
        self.read_conf_file(conf=conf_file)
        self.session = FacebookAdsApi.init(app_id=self.app_id, app_secret=self.app_secret, access_token=self.access_token,
                                           api_version=self.api_version)

    def account_list(self):
        me = User(fbid='me')
        self.AD_account_list = [value['account_id'] for value in me.get_ad_accounts()]

    def extract_api_cost(self, data_start, date_end):
        self.account_list()
        lst_args = []
        print(f"Has been started computation for range {date_end} {data_start}\nLen of AD_account_list = {len(self.AD_account_list)}"
              f"\nCPU = {mp.cpu_count()}\nAD_account_list={self.AD_account_list}")

        for each_ad_account in self.AD_account_list:
            lst_args.append((each_ad_account, self.api_version, self.access_token, data_start, date_end))

        pool = mp.Pool(processes=(mp.cpu_count()))
        results = pool.map(return_adaccount_dataframe, lst_args)
        pool.close()
        pool.join()
        self.df_all_cost = pd.concat([each_df for each_df in results if each_df.shape[0] > 0])

    def transform_api_cost(self):
        self.df_all_cost.replace(to_replace=['NA', None, np.inf, np.nan, ''], value=0, inplace=True)

        if 'spend' in self.df_all_cost.columns:
            self.df_all_cost['spend'] = pd.to_numeric(self.df_all_cost['spend'], errors='coerce').fillna(0).astype('Float64')

        for each_type in ['impressions', 'clicks', 'campaign_id', 'ad_id', 'adset_id', 'mobile_app_install', 'omni_app_install']:
            if each_type in self.df_all_cost.columns:
                self.df_all_cost[each_type] = pd.to_numeric(self.df_all_cost[each_type], errors='coerce').fillna(0).astype('Int64')

        self.df_all_cost.replace(to_replace=['NA', None, np.inf, np.nan, ''], value=0, inplace=True)
        self.df_all_cost.rename(columns={'account_id': 'app'}, inplace=True)

        if Facebook_accounts_links != None:
            self.df_all_cost.replace({'app': Facebook_accounts_links}, inplace=True)


    @benchmark_info()
    def load_api_cost(self, data_start, date_end):
        self.extract_api_cost(data_start, date_end)
        self.transform_api_cost()
        return self.df_all_cost.reset_index(drop=True)

    def read_conf_file(self, conf='facebook.conf'):
        with open(os.path.join(LOCAL_FILE_PATH, conf), 'r') as file:
            configuration = eval(file.read())
        try:
            self.api_version = configuration['fb_api_version']
            self.app_id = configuration['app_id']
            self.app_secret = configuration['app_secret']
            self.access_token = configuration['access_token']
        except:
            raise ValueError("facebook.conf had wrong configuration")


if __name__ == '__main__':
    pass

