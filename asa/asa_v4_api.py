import os,sys, requests, re, traceback, urllib3

import pandas as pd

urllib3.disable_warnings()

LOCAL_FILE_PATH = os.path.dirname(os.path.abspath(__file__))
GLOBAL_PATH = os.path.join(LOCAL_FILE_PATH,'..','..')
sys.path.insert(0, os.path.join(GLOBAL_PATH))

from modules.variables import APPLICATION_NAME_DICT

def _return_map_app_name(x):
    for each_app in APPLICATION_NAME_DICT.keys():
        if each_app.lower() in str(x).lower():
            return APPLICATION_NAME_DICT[each_app]
    return str(x)


class ASA_data_API():
    asa_v4_token_url = 'https://appleid.apple.com/auth/oauth2/token'
    asa_v4_token_head = {"Content-Type": "application/x-www-form-urlencoded", "Host": "appleid.apple.com"}
    asa_v4_campaign_list_url = 'https://api.searchads.apple.com/api/v4/campaigns'
    asa_v4_campaigns_reports_url = 'https://api.searchads.apple.com/api/v4/reports/campaigns'
    asa_v4_adgroups_url = 'https://api.searchads.apple.com/api/v4/campaigns/{}/adgroups/targetingkeywords/find'
    asa_v4_params_CAMP = {'limit': 1000}

    asa_v4_params_TKW = {"pagination": {"offset": 0, "limit": 2000},
                         "orderBy": [{"field": "id", "sortOrder": "ASCENDING"}],
                         "conditions": [{"field": "deleted", "operator": "EQUALS",
                                         "values": ["false"]}]}

    params_asa_v4 = '{{ "startTime": "{0}", \
                         "endTime": "{1}", \
                         "granularity": "DAILY", \
                "selector": {{ \
                    "orderBy":[ \
                        {{ \
                            "field": "countryOrRegion", \
                            "sortOrder": "ASCENDING" \
                        }}], \
                    "pagination": {{ \
                        "offset": 0, \
                        "limit": 1000 \
                    }} }}, \
                "groupBy":[ \
                    "countryOrRegion", \
                    "deviceClass"], \
                "timeZone": "UTC", \
                "returnRecordsWithNoMetrics": False, \
                "returnRowTotals": False, \
                "returnGrandTotals": False}}'

    def __init__(self, orgId):

        self.get_asa_config()

        self.asa_v4_token_params = {"grant_type": "client_credentials",
                                    "client_id": self.asa_config['client_id'],
                                    "client_secret": self.asa_config['client_secret'],
                                    "scope": "searchadsorg"}

        self.get_token()

        self.asa_headers_main_access = {"Authorization": "Bearer {}".format(self.asa_token),
                                        "X-AP-Context": "orgId={}".format(orgId)}


    def get_token(self):
        response = requests.post(url=self.asa_v4_token_url,
                                 params=self.asa_v4_token_params,
                                 headers=self.asa_v4_token_head)

        if response.status_code == 200:
            self.asa_token = response.json()['access_token']
        else:
            self.asa_token = ''


    def get_asa_config(self):
        with open(os.path.join(LOCAL_FILE_PATH, 'asa_client_secret.json'), 'r') as file:
            self.asa_config = eval(file.read())


    def get_campaings_list(self):
        try:
            response_campaings = requests.get(url=self.asa_v4_campaign_list_url,
                                              headers=self.asa_headers_main_access,
                                              params=self.asa_v4_params_CAMP)

            campaigns_df = pd.json_normalize(response_campaings.json()['data'])
        except:
            campaigns_df = pd.DataFrame()
        finally:
            return campaigns_df


    def get_api_data(self, start_date, end_date):

        full_dataset = pd.DataFrame()

        try:
            campaigns_df = self.get_campaings_list()
            campaigns_list = list(campaigns_df['id'].values)
            print("CAMPAIGNS LIST [{}] = {}".format(len(campaigns_list), campaigns_list))

            for each_campaign in campaigns_list:

                request = requests.post(url='{}/{}/adgroups'.format(self.asa_v4_campaigns_reports_url, each_campaign),
                                        headers=self.asa_headers_main_access,
                                        json=eval(self.params_asa_v4.format(start_date, end_date)), verify=False)

                if request.status_code == 200:

                    dataset = pd.json_normalize(request.json()['data']['reportingDataResponse']['row'])

                    if dataset.shape[0] > 0:
                        try:
                            full_dataset = full_dataset.append(dataset, sort=False)
                        except UnboundLocalError:
                            full_dataset = dataset

                request = requests.post(self.asa_v4_adgroups_url.format(each_campaign),
                                        headers=self.asa_headers_main_access,
                                        json=self.asa_v4_params_TKW,
                                        verify=False)

                if request.status_code == 200:
                    targetingkeywords_df = pd.json_normalize(request.json()['data'])

                    if targetingkeywords_df.shape[0] > 0:
                        try:
                            full_targetingkeywords_df = full_targetingkeywords_df.append(targetingkeywords_df, sort=False)
                        except UnboundLocalError:
                            full_targetingkeywords_df = targetingkeywords_df

            mapper_df = full_targetingkeywords_df.groupby('adGroupId').agg(keywords=('text', 'unique')).reset_index()

            mapper_keywords = dict(zip(mapper_df['adGroupId'], mapper_df['keywords']))

            full_dataset = full_dataset.reset_index(drop=True)

            full_dataset['granularity'] = full_dataset['granularity'].apply(lambda x: x[0])

            full_dataset = pd.merge(full_dataset, pd.json_normalize(full_dataset['granularity']), left_index=True, right_index=True)

            full_dataset.rename(columns={'metadata.campaignId': 'campaign_id',
                                         'metadata.deviceClass': 'device',
                                         'metadata.countryOrRegion': 'country_code',
                                         'metadata.adGroupId': 'group_id',
                                         'metadata.adGroupName': 'group_name',
                                         'localSpend.amount': 'spend',
                                         'date': 'start_date',
                                         'taps': 'clicks'}, inplace=True)

            full_dataset.columns = [re.sub('[^a-zA-Z0-9]', '_', value.lower()) for value in full_dataset.columns.tolist()]

            campaign_dict = {x['id']: x['name'] for x in campaigns_df[['id', 'name']].to_dict('records')}

            full_dataset['campaign_name'] = full_dataset['campaign_id'].map(campaign_dict)
            full_dataset['keywords'] = full_dataset['group_id'].map(mapper_keywords)

            if 'campaign_name' in full_dataset.columns:
                full_dataset['app'] = full_dataset['campaign_name'].apply(lambda x: _return_map_app_name(x))

            full_dataset['spend'] = pd.to_numeric(full_dataset['spend'])
            full_dataset['installs'] = pd.to_numeric(full_dataset['installs'])
            full_dataset['start_date'] = pd.to_datetime(full_dataset['start_date'])

            full_dataset.drop(columns=['granularity'], inplace=True)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            func_name = traceback.extract_tb(sys.exc_info()[-1], 1)[0][2]
            print('[{}] Has been erased next error: [Line={}] Type=[{}] Obj=[{}]'.format(func_name, exc_tb.tb_lineno, exc_type, exc_obj))

        finally:
            return full_dataset

if __name__ == '__main__':
    pass