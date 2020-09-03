# Need to install requests package for python
# easy_install requests
import requests
import pandas
import numpy as np
import json
import re
import yaml


def LoadAndConfigure():
    global account_id
    global api_token
    global log_level
    global apm_json
    global infra_json
    global snow_url
    global snow_login
    global snow_pwd
    with open('nr_snow.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        account_id = config["account_id"]
        api_token = config["api_token"]
        log_level = config["log_level"]
        snow_url = config["snow_url"]
        snow_login = config["snow_login"]
        snow_pwd = config["snow_pwd"]

    print("Successfully loaded config file")


LoadAndConfigure()

infra_dict = {"u_apmapplicationids": "", "u_awsregion": "", "u_corecount": "", "u_fullhostname": "", "u_guid": "",
              "u_instancetype": "", "name": "", "u_operatingsystem": "", "u_processorcount": "", "u_systemmemorybytes": ""}

df_infra = pandas.read_csv('infra_data_file.csv')
df_infra.fillna('', inplace=True)

# Set the request parameters
url = snow_url + '/api/now/table/u_cmdb_ci_new_relic_infra'


# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}
count = 1
for index, row in df_infra.iterrows():

    infra_dict["u_apmapplicationids"] = re.sub(
        '[\[|\]\']', '', str(row["apmApplicationIds"]))
    infra_dict["u_awsregion"] = row["awsRegion"]
    infra_dict["u_corecount"] = re.sub('[\[|\]\']', '', str(row["coreCount"]))
    infra_dict["u_fullhostname"] = re.sub(
        '[\[|\]\']', '', str(row["fullHostname"]))
    infra_dict["u_guid"] = re.sub('[\[|\]\']', '', str(row["guid"]))
    infra_dict["u_instancetype"] = re.sub(
        '[\[|\]\']', '', str(row["instanceType"]))
    infra_dict["name"] = re.sub('[\[|\]\']', '', str(row["name"]))
    infra_dict["u_operatingsystem"] = re.sub(
        '[\[|\]\']', '', str(row["operatingSystem"]))
    infra_dict["u_processorcount"] = re.sub(
        '[\[|\]\']', '', str(row["processorCount"]))
    infra_dict["u_systemmemorybytes"] = re.sub(
        '[\[|\]\']', '', str(row["systemMemoryBytes"]))

    count += 1

    infra_json = json.dumps(infra_dict)

    print(infra_json)
# Do the HTTP request
    response = requests.post(url, auth=(
        snow_login, snow_pwd), headers=headers, data=infra_json)

# Check for HTTP codes other than 200
    if response.status_code > 201:
        print('Status:', response.status_code, 'Headers:',
              response.headers, 'Error Response:', response.json())
        exit()


# Decode the JSON response into a dictionary and use the data
data = response.json()


# Insert INFRA data into CMDB ends

# Insert APM data into CMDB

apm_dict = {"u_guid": "", "name": "", "u_language": "", "u_errorrate": "",
            "u_responsetimeaverage": "", "u_throughput": "", "u_hostcount": ""}

df_apm = pandas.read_csv('apm_data_file.csv')
df_apm.fillna('', inplace=True)

# Set the request parameters
url = snow_url + '/api/now/table/u_cmdb_ci_new_relic_apm'

# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}
count = 1
for index, row in df_apm.iterrows():
    apm_dict["u_guid"] = re.sub('[\[|\]\']', '', str(row["guid"]))
    apm_dict["u_language"] = re.sub('[\[|\]\']', '', str(row["language"]))
    apm_dict["u_errorrate"] = re.sub(
        '[\[|\]\']', '', str(row["apmSummary.errorRate"]))
    apm_dict["u_responsetimeaverage"] = re.sub(
        '[\[|\]\']', '', str(row["apmSummary.responseTimeAverage"]))
    apm_dict["u_throughput"] = re.sub(
        '[\[|\]\']', '', str(row["apmSummary.throughput"]))
    apm_dict["name"] = re.sub('[\[|\]\']', '', str(row["name"]))
    apm_dict["u_hostcount"] = re.sub(
        '[\[|\]\']', '', str(row["apmSummary.hostCount"]))
    count += 1
    apm_json = json.dumps(apm_dict)

# Do the HTTP request
    response = requests.post(url, auth=(
        snow_login, snow_pwd), headers=headers, data=apm_json)

# Check for HTTP codes other than 201
    if response.status_code > 201:
        print('Status:', response.status_code, 'Headers:',
              response.headers, 'Error Response:', response.json())
        exit()


# Decode the JSON response into a dictionary and use the data
data = response.json()

# Insert APM data into CMDB ends


# ************************************************************************

# Set up relationships in CMDB

df_NR = pandas.read_csv('relationship_data_file.csv')
df_NR.fillna('', inplace=True)
# Set the request parameters
url = snow_url + '/api/now/table/u_cmdb_ci_new_relic_infra?sysparm_fields=sys_id%2C%20u_guid&sysparm_limit=60'


# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Do the HTTP request
response = requests.get(url, auth=(snow_login, snow_pwd), headers=headers)

# Check for HTTP codes other than 200
if response.status_code != 200:
    print('Status:', response.status_code, 'Headers:',
          response.headers, 'Error Response:', response.json())
    exit()

data = response.json()
df_SNOW = pandas.json_normalize(data["result"])

df_All = df_SNOW.merge(df_NR, left_on="u_guid", right_on="source.entity.guid")
df_All.drop_duplicates()
df_All.fillna('', inplace=True)
df_All.to_csv("concatenate2.csv", index=False)

# Get APM CIs from SNOW

# Set the request parameters
url = snow_url + '/api/now/table/u_cmdb_ci_new_relic_apm?sysparm_fields=sys_id%2C%20u_guid&sysparm_limit=60'


# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Do the HTTP request
response = requests.get(url, auth=(snow_login, snow_pwd), headers=headers)

print("Response is:" + response.text)
# Check for HTTP codes other than 200
if response.status_code > 201:
    print('Status:', response.status_code, 'Headers:',
          response.headers, 'Error Response:', response.json())
    exit()

# Decode the JSON response into a dictionary and use the data
data2 = response.json()
df_apm = pandas.json_normalize(data2["result"])
df_apm.fillna('', inplace=True)
# print(df_apm)

# Setting up CI relationships

rel_url = snow_url + "/api/now/cmdb/instance/u_cmdb_ci_new_relic_infra/"

# Set proper headers
headers = {"Content-Type": "application/json", "Accept": "application/json"}

# Do the HTTP request
# response = requests.post(rel_url, auth=(user, pwd), headers=headers ,data=

dictRelation = {"inbound_relations": [{"type": "5f985e0ec0a8010e00a9714f2a172815",
                                       "target": "replacesysidhere", "sys_class_name": "cmdb_ci"}], "source": "NewRelic"}

for index_all, row_all in df_All.iterrows():
    final_rel_url = rel_url + str(row_all["sys_id"]) + "/relation"

    for index_apm, row_apm in df_apm.iterrows():
        if row_all["target.entity.guid"] == row_apm["u_guid"]:
            target_sysid = row_apm["sys_id"]

            print(target_sysid)
            strRelation = dictRelation["inbound_relations"][0]
            strRelation["target"] = target_sysid
            # Do the HTTP request
            r = json.dumps(dictRelation)
            s = json.loads(r)
            response = requests.post(final_rel_url, auth=(
                snow_login, snow_pwd), headers=headers, data=r)
            #print (response)
        # Check for HTTP codes other than 200
            if response.status_code > 201:
                print('Status:', response.status_code, 'Headers:',
                      response.headers, 'Error Response:', response.json())
                exit()
