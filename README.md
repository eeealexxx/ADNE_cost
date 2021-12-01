# ADNE_cost

## Scripts for collecting cost from diversity advertising networks

## ASA network

orgID = '**12345678**' special 8 digital code of orgId in Aplle Search Advertise network

For obtained dataframe with cost your campaigns, you have to run next:

_**df_cost = ASA_data_API(orgID).get_api_data('2021-11-11','2021-11-11')**_

Detail information from developer site: https://developer.apple.com/documentation/apple_search_ads/implementing_oauth_for_the_apple_search_ads_api

## Facebook network

All necessary parameters are present in **facebook.conf** which is located in the same directory with the main script. 
For obtained dataframe with cost your campaigns, you have to run next:

_**df_cost = Facebook_API_cost().load_api_cost('2021-12-01','2021-12-01')**_

