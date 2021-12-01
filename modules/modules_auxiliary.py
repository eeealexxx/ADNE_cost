import pandas as pd
import numpy as np
import requests, urllib3, json, datetime

from time import sleep
from pandas import json_normalize
from dateutil.relativedelta import relativedelta

urllib3.disable_warnings()


def benchmark_info(info=True):
    def decorator(f):
        def wrapper(*args, **kw):
            start = datetime.datetime.now()
            res = f(*args, **kw)
            diff = relativedelta(datetime.datetime.now(), start)

            if info:
                print("Function [{3}] has been spent {0} hours {1} min {2} sec.".format(diff.hours, diff.minutes, diff.seconds, f.__name__))
            return res
        return wrapper
    return decorator


def return_adaccount_dataframe(args):
    try:
        each_ad_account, fb_api_version, access_token, s_date, e_date = args
        print("Has been started computation for {}".format(each_ad_account))

        df_all_campaings = pd.DataFrame()
        url = f"https://graph.facebook.com/{fb_api_version}/act_{each_ad_account}/campaigns"

        params_campaigns = {"access_token": access_token,
                            "limit": 100,
                            "fields": "name,objective,effective_status"}

        df_campaings = return_all_paging_values_FB_v2(url, params_campaigns, each_ad_account=each_ad_account)

        if df_campaings.shape[0] > 0:
            main_campaigns_list = df_campaings['id'].unique().tolist()

            for each_campaings in main_campaigns_list:
                url = f"https://graph.facebook.com/{fb_api_version}/{each_campaings}/insights"
                params_each_campaign = {"access_token": access_token, "level": "ad", "breakdowns": "country", \
                                        "period": "day",
                                        "fields": "account_id,account_name,unique_actions,impressions,clicks,spend,"
                                                  "campaign_name,adset_name,ad_id,ad_name,adset_id,campaign_id", \
                                        "time_range": "{{'since':'{0}','until':'{1}'}}".format(s_date, e_date), \
                                        "limit": 1000
                                        }
                df_temp = return_all_paging_values_FB_v2(url, params_each_campaign, each_ad_account=each_ad_account)

                if df_temp.shape[0] > 0:
                    """
                    Transform dataframe from API
                    """
                    for each_fields in ['spend']:
                        if each_fields not in df_temp.columns:
                            df_temp[each_fields] = 0
                    full_temp = pd.DataFrame()
                    if 'unique_actions' not in df_temp.columns:
                        df_temp['mobile_app_install'] = 0
                        df_temp['omni_app_install'] = 0
                    else:
                        df_temp['unique_actions'] = df_temp['unique_actions'].apply(lambda x: [{'action_type': 'video_view', 'value': '0'}] if len(str(x)) <= 3 else x)
                        for i in range(df_temp.shape[0]):
                            temp_ = pd.DataFrame(df_temp.loc[i, ('unique_actions')])
                            try:
                                full_temp = full_temp.append(pd.DataFrame(temp_['value'].values, temp_['action_type'].values).T, sort=False)
                            except UnboundLocalError:
                                full_temp = pd.DataFrame(temp_['value'].values, temp_['action_type'].values).T

                        full_temp = full_temp.reset_index(drop=True)

                        for each_fields in ['mobile_app_install', 'omni_app_install']:
                            if each_fields not in full_temp.columns:
                                full_temp[each_fields] = 0

                        df_temp = pd.merge(df_temp, full_temp[['mobile_app_install', 'omni_app_install']], left_index=True, right_index=True)

                    try:
                        df_all_campaings = df_all_campaings.append(df_temp, sort=False)
                    except UnboundLocalError:
                        df_all_campaings = df_temp

            df_all_campaings['ad_account_id'] = each_ad_account

        print("Has been finished computation for {}".format(each_ad_account))

    except Exception as e:
        print(e)
        return pd.DataFrame()
    else:
        return df_all_campaings


def return_all_paging_values_FB_v2(url, params, l_timeout=180, each_ad_account='None'):
    df_all = pd.DataFrame()
    repeat = 1

    while True:
        try:
            while True:
                response_ = requests.get(url, params=params, timeout=l_timeout)
                if response_.status_code == 200:
                    break
                else:
                    try:
                        response_H = json.loads(response_.headers['x-business-use-case-usage'])
                        sleep_time = response_H[each_ad_account][0]['estimated_time_to_regain_access']
                        print("status_code={}, ad_account={}, sleep_time={}".format(response_.status_code, each_ad_account, sleep_time))
                        sleep(sleep_time * 60)
                    except Exception as e:
                        print(e)
        except requests.exceptions.ConnectionError as e:
            response_ = None
            print(e)

        if response_ != None or repeat == 5:
            break
        repeat += 1

    while True:
        try:
            try:
                df_all = df_all.append(json_normalize(response_.json()['data']))
            except:
                df_all = json_normalize(response_.json()['data'])

            repeat = 1

            while True:
                try:
                    while True:
                        response_ = requests.get(response_.json()['paging']['next'], timeout=l_timeout)
                        if response_.status_code == 200:
                            break
                        else:
                            try:
                                response_H = json.loads(response_.headers['x-business-use-case-usage'])
                                sleep_time = response_H[each_ad_account][0]['estimated_time_to_regain_access']
                                print("M2 status_code={}, ad_account={}, sleep_time={}".format(response_.status_code, each_ad_account, sleep_time))
                                sleep(sleep_time * 60)
                            except Exception as e:
                                print(e)
                except requests.exceptions.ConnectionError as e:
                    response_ = None
                    print(e)
                if response_ != None or repeat == 5:
                    break
                repeat += 1
        except KeyError as e:
            break
    return df_all.reset_index(drop=True)
