# ADNE_cost

### Scripts for collecting cost from diversity advertising networks

orgID = '12345678' special 8 digital code of orgId in Aplle Search Advertise network

For obtained dataframe with cost you campaigns you have to run next:

df_cost = ASA_data_API(orgID).get_api_data('2021-11-11','2021-11-11')
