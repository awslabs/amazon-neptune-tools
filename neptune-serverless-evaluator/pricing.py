#!/usr/bin/python3

# The following assumptions are made while calculating the cost of running Serverless V2
# CPU is 1/8th of the memory can be adjusted by the variable ncu_cpu
# Cost estimation is on the CPU utilization as free memory is not a great indicator of memory consumption
# Scale up and down operation matches with the current cpu utilization and no warmup or cool off period
# 

import boto3
import argparse
import datetime, math
import sys
import traceback
import json
from operator import itemgetter
import dateutil
from bs4 import BeautifulSoup
import requests
import numpy as np

global args

ncu_memory = 2048
ncu_cpu = 0.25
ncu_network = 0.09375
ncu_cost = 0.1608
metrics = {"CPU": "CPUUtilization", "Network": 'NetworkThroughput', "NCU":"ServerlessDatabaseCapacity"}

ondemand_instances = [{"type":"db.r6g.large", "vcpu":2},
                      {"type":"db.r6g.xlarge", "vcpu":4},
                      {"type":"db.r6g.2xlarge", "vcpu":8},
                      {"type":"db.r6g.4xlarge", "vcpu":16},
                      {"type":"db.r6g.8xlarge", "vcpu":32},
                      {"type":"db.r6g.16large", "vcpu":64}]

def get_cw_metrics(metricnamespace, metricname, start_date, end_date):

    namespace = "AWS/Neptune"

    cloudwatch = boto3.client('cloudwatch',region_name=args.region)
    try:
        response = cloudwatch.get_metric_statistics(
                  Namespace=namespace,
                  MetricName=metricname,
                  Dimensions=[
                   {
                    "Name": "DBInstanceIdentifier",
                    "Value": args.name
                   },
                  ],
                 StartTime=start_date,
                 EndTime=end_date,
                 Period=60,
                 Statistics=["Maximum"]
                 )
        
        sortList = []     
        for item in response['Datapoints']:
            sortList.append(item)
            sortList = sorted(sortList, key=itemgetter('Timestamp'))

        #print("Lent of list {}".format(len(sortList)))
        return sortList

    except Exception as e:
        traceback.print_exception(*sys.exc_info())
        print(e)


def get_metrics():

    output = {}
    for metric in metrics:
        newlist =[]
        for i in range(0,int(args.period)):
            now = datetime.datetime.utcnow() - datetime.timedelta(days=i)
            start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = now.replace(hour=23, minute=59, second=59, microsecond=0)
            newlist = newlist + get_cw_metrics(metric, metrics[metric],start_date,end_date)
        output[metric] = newlist
    return output 

#get price of the instance based upon NCU utilized
def ondemand_price(ncu_utilized, hrs):
    vcpu_required = math.ceil(ncu_utilized / 4)
    matching_instance = {}
    for i in ondemand_instances:
        if vcpu_required < i["vcpu"]:
            matching_instance = i
            break

    filters = [{ 'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': args.region },
              { 'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': matching_instance["type"] }]
  
    
    client = boto3.client('pricing', region_name="us-east-1")
    data = client.get_products(
        ServiceCode='AmazonNeptune',
        Filters=filters
        )
    
    hrs = math.ceil(hrs)
    inst = json.loads(data['PriceList'][0])
    inst = inst['terms']['OnDemand']
    inst = list(inst.values())[0]
    inst = list(inst["priceDimensions"].values())[0]
    inst = inst['pricePerUnit']
    args.currency = list(inst.keys())[0]
    instance_cost = float(inst[args.currency])    
    return {"type": matching_instance["type"], "total_cost": round(instance_cost * hrs,3 )}



def get_metric_val(output, metric, dt):
    sample = [ x for x in output[metric] if x['Timestamp'] == dt ]
    return sample

def parse_metrics(output):

    newoutput = []
    total_ncu_cost = 0
    datapoints = 0
    max_ncu = 0
    min_ncu = 500 
    total_ncu = 0

    for metrics in output['CPU']:
        actual_cpu = metrics['Maximum'] * int(args.instance_cpu) /100
        ncu = round(actual_cpu/ncu_cpu)

        # NCU with respect to NetworkThroughput
        for network in [ x for x in output['Network'] if x['Timestamp'] == metrics['Timestamp']]:
           network_kb = round( network['Maximum'] / 1024 )
           if network_kb < 6292:
              network_ncu = 0.5
           else:
              network_ncu = least( greatest(1,  round( network_kb / 12582 )), 128)
           ncu = max(ncu, network_ncu)

        if ncu == 0 :
           ncu = 0.5
        if ncu > max_ncu :
            max_ncu = ncu
        if ncu < min_ncu :
            min_ncu = ncu
        total_ncu = total_ncu + ncu
        total_ncu_cost = total_ncu_cost + ncu*ncu_cost/60
        out1 = {"Timestmap" : metrics['Timestamp'], "cpu":metrics['Maximum'], "ncu": ncu }
        newoutput.append(out1)
        datapoints = datapoints + 1		

    total_instance_cost = float(args.instance_cost) * datapoints / 60 
    print("Region : {}".format(args.region))
    print("Instance name : {}".format(args.name))
    print("Instance type : {}".format(args.instance_type))
    print("Data collection period : {} days".format(args.period))
    print("Cost of running on-demand provisioned instance : ${}/hr".format(args.instance_cost))
    print("Total cost of running provisioned for {} days : ${}".format(args.period, round(total_instance_cost,2)))
    print("Total cost of running serverless for {} days : ${}".format(args.period, round(total_ncu_cost,2)))
    print("Maximum NCU utilization : {}".format(max_ncu))
    print("Minimum NCU utilization : {}".format(min_ncu))
    print("Average NCU utilization : {}".format(round(total_ncu/datapoints)))
    print("Total data points : {}".format(datapoints))

def parse_serverless_cw_logs(output):
    max_ncus = [i['Maximum'] for i in output['NCU']]
    total_ncu_cost = []
    total_ncu_cost = sum([(ncu * args.instance_cost/60) for ncu in max_ncus])
    datapoints = len(max_ncus)
    total_ncus = sum(max_ncus)
    avg_ncu = round(total_ncus/datapoints)
    avg_ncu_od = ondemand_price(avg_ncu, datapoints / 60)


    min_ncu = min(max_ncus)
    min_ncu_od = ondemand_price(min_ncu, datapoints / 60)

    max_ncu = max(max_ncus)
    max_ncu_od = ondemand_price(max_ncu, datapoints / 60)

    nineth_pect_ncu = np.percentile(max_ncus,90)
    nineth_pect_ncu_od = ondemand_price(nineth_pect_ncu, datapoints / 60)

    print("Region : {}".format(args.region))
    print("Instance name : {}".format(args.name))
    print("Instance type : {}".format(args.instance_type))
    print("Data collection period : {} days".format(args.period))
    print("Total cost of running serverless for last {} days : ${}".format(args.period, round(total_ncu_cost,2)))
    print("Minimum NCU utilization : {}".format(min_ncu),",Equivalent OnDemand Instance Costs: ("+ min_ncu_od["type"] +") $"+ str(min_ncu_od["total_cost"]))
    print("Maximum NCU utilization : {}".format(max_ncu),",Equivalent OnDemand Instance Costs: ("+ max_ncu_od["type"] +") $"+ str(max_ncu_od["total_cost"]))    
    print("90th Percentile NCU Utilization : {}".format(nineth_pect_ncu),",Equivalent OnDemand Instance Costs: ("+ nineth_pect_ncu_od["type"] +") $"+ str(nineth_pect_ncu_od["total_cost"]))
    print("Average NCU utilization : {}".format(avg_ncu),",Equivalent OnDemand Instance Costs: ("+ avg_ncu_od["type"] +") $"+ str(avg_ncu_od["total_cost"]))
    print("Total data points : {}".format(datapoints))
    
   

def get_instance_details():

    global args

    filters = []
    try:
        rds = boto3.client("neptune", region_name=args.region)
        response = rds.describe_db_instances(DBInstanceIdentifier=args.name)
        args.instance_type =response['DBInstances'][0]['DBInstanceClass']
        filters.append({ 'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': args.region })
    except:
        print("Unable to gather instance information. Instance may not be present in the given region")
        sys.exit(1)
    if args.instance_type is None:
        print("Unable to gather instance information. Instance may not be present in the given region")
        sys.exit(1)    
    elif args.instance_type == 'db.serverless':
        filters.append( { 'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': 'Serverless' })
    else:
        filters.append({ 'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': args.instance_type })
    
    client = boto3.client('pricing', region_name="us-east-1")
    data = client.get_products(
        ServiceCode='AmazonNeptune',
        Filters=filters
        )

    inst = json.loads(data['PriceList'][0])
    
    if args.instance_type != 'db.serverless':
        args.instance_memory = inst['product']['attributes']['memory'].split()[0]
        args.instance_cpu = inst['product']['attributes']['vcpu']
    inst = inst['terms']['OnDemand']
    inst = list(inst.values())[0]
    inst = list(inst["priceDimensions"].values())[0]
    inst = inst['pricePerUnit']
    args.currency = list(inst.keys())[0]
    args.instance_cost = float(inst[args.currency])


def get_param():

    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--name", action="store", default=None, help="Instance name")
    parser.add_argument("-r", "--region", action="store", default=None, help="Region")
    parser.add_argument("-p", "--period", action="store", default=14, help="Period for calculation. Default 2 weeks")
    args = parser.parse_args()

    if args.name is None or args.region is None:
        print("Invalid parameter passed")
        sys.exit(1)

def main():

   output = {}
   get_param()
   get_instance_details()
   output = get_metrics()
   #print(output)
   if args.instance_type == 'db.serverless':
       parse_serverless_cw_logs(output)
   else:
       parse_metrics(output)
   
   


if __name__ == "__main__":
    main()
