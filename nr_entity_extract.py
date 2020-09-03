import requests
import json
import csv
import pandas
from pandas.io.json import json_normalize
import yaml
import math


# GraphQL query for APM CIs
apm_json = {"query": "{\n  actor {\n    entitySearch(query: \"domain IN (\u0027APM\u0027) and accountId = replaceaccountidhere\") {\n      results {\n        nextCursor\n        entities {\n          ... on ApmApplicationEntityOutline {\n            guid\n            name\n          tags {\n              key\n              values\n            }\n          apmSummary {\n              errorRate\n              hostCount\n              throughput\n              responseTimeAverage\n              webThroughput\n            }\n            applicationId\n            entityType\n          }\n        }\n      }\n    }\n  }\n}\n", "variables": ""}

# GraphQL query for Infra CIs
infra_json = {"query": "{\n  actor {\n    entitySearch(query: \"domain IN (\u0027INFRA\u0027) and accountId = replaceaccountidhere\") {\n      results {\n        nextCursor\n        entities {\n          ... on InfrastructureHostEntityOutline {\n            guid\n            hostSummary {\n              cpuUtilizationPercent\n              diskUsedPercent\n              memoryUsedPercent\n              networkReceiveRate\n              networkTransmitRate\n              servicesCount\n            }\n            name\n            entityType\n            domain\n   tags {\n              key\n              values\n            }\n            type\n        }\n        }\n      }\n    }\n  }\n}\n", "variables": ""}

# GraphQL query for relationship between APM and Infra
relationship_json = {"query": "{\n  actor {\n    entity(guid: \"replaceguidhere\") {\n      relationships(filter: {entityType: INFRASTRUCTURE_HOST_ENTITY}) {\n        source {\n          entityType\n          guid\n          entity {\n            name\n  guid\n  entityType\n       }\n        }\n        target {\n          entity {\n            name\n   guid\n  entityType\n       }\n        }\n      }\n    }\n  }\n}\n", "variables": ""}

# Loads yaml config file and 'preps' the jsons


def LoadAndConfigure():
    global account_id
    global api_token
    global log_level
    global apm_json
    global infra_json
    with open('nr_snow.yaml', 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
        account_id = config["account_id"]
        api_token = config["api_token"]
        log_level = config["log_level"]

    # Edit APM json with correct account id
    apm_str = apm_json["query"]
    updated_apm_str = apm_str.replace("replaceaccountidhere", str(account_id))
    apm_json["query"] = updated_apm_str

    # Edit APM json with correct account id
    infra_str = infra_json["query"]
    updated_infra_str = infra_str.replace(
        "replaceaccountidhere", str(account_id))
    infra_json["query"] = updated_infra_str

    print("Successfully loaded config file")

# Extracts CIs for both APM and Infra


def ExtractNewRelicCI(json_input, filename, type):

    url = 'https://api.newrelic.com/graphql'
    print(api_token)
    headers = {'api-key': api_token}

    r = requests.post(url=url, json=json_input, headers=headers)
    rjson = r.json()

    data_orig = rjson['data']['actor']['entitySearch']['results']['entities']

    df = pandas.json_normalize(data_orig).drop_duplicates(subset='name')
    df.dropna()
    mydict = {}
    nan_value = float("nan")
    df.replace("", nan_value, inplace=True)

    # Commented to handle tags
    header_list = ['fullHostname', 'processorCount', 'coreCount', 'operatingSystem',
                   'systemMemoryBytes', 'awsRegion', 'instanceType', 'apmApplicationIds', 'Environment', 'language']
    count = 0
    for index, row in df.iterrows():
        mydict = {}
        mydict["guid"] = str(row["guid"])
        mydict["name"] = str(row["name"])
        if (type == 'INFRA'):
            mydict["fullHostname"] = ""
            mydict["processorCount"] = ""
            mydict["coreCount"] = ""
            mydict["operatingSystem"] = ""
            mydict["systemMemoryBytes"] = ""
            mydict["awsRegion"] = ""
            mydict["Environment"] = ""
            mydict["instanceType"] = ""
            mydict["apmApplicationIds"] = ""
        else:
            mydict["language"] = ""
            mydict["apmSummary.errorRate"] = str(row["apmSummary.errorRate"])
            mydict["apmSummary.responseTimeAverage"] = str(
                row["apmSummary.responseTimeAverage"])
            mydict["apmSummary.throughput"] = str(row["apmSummary.throughput"])
            mydict["apmSummary.hostCount"] = str(row["apmSummary.hostCount"])
        if (str(row["tags"]) != 'nan'):
            for j in row["tags"]:
                if (j['key'] in header_list):
                    mydict[j['key']] = j['values'].copy()
            if (count == 0):
                pandas.json_normalize(mydict).to_csv(
                    filename, index=False, mode='a')
                count = 1
            else:
                pandas.json_normalize(mydict).to_csv(
                    filename, index=False, mode='a', header=False)
    # Commented to handle tags
    print("Successfully extracted CI")
    return df

# Extracts relationships between APM and Infra CIs


def ExtractNewRelicAppGuids(json_input, filename, apmDataFrame):

    url = 'https://api.newrelic.com/graphql'
    headers = {'api-key': api_token}

    count = 0
    guidlist = apmDataFrame["guid"]
    for gd in guidlist:
        print("Procesing relationship info for GUID: " + gd)
        newStr = ""
        tempStr = ""
        temp_json = json_input.copy()
        tempStr = temp_json["query"]
        newStr = tempStr.replace("replaceguidhere", gd)
        temp_json["query"] = newStr
        r = requests.post(url=url, json=temp_json, headers=headers)
        rjson = r.json()
        data = rjson['data']['actor']['entity']['relationships']
        df = pandas.json_normalize(data)
        if count == 0:
            df.to_csv(filename, mode='a',  index=False)
            count += 1
        else:
            df.to_csv(filename, mode='a', header=None, index=False)
        temp_json.clear()
    print("Successfully extracted relationship info")


# Load and configure
LoadAndConfigure()

# Call function to extract APM CIs
apmDataFrame = ExtractNewRelicCI(apm_json, 'apm_data_file.csv', 'APM')

# Call function to extract Infra CIs
ExtractNewRelicCI(infra_json, 'infra_data_file.csv', 'INFRA')

# Call function to get relationships between APM and Infra
ExtractNewRelicAppGuids(
    relationship_json, 'relationship_data_file.csv', apmDataFrame)
