## Neptune Serverless Cost Evaluator
Neptune offers On-Demand provisioned instances and serverless instances which to accommodate variety of scaling up or down needs.   Choosing between 2 modes is often a decision of cost and requires understanding access patterns. I will walk through some of the decision factors for new workloads and also show how you can use cloudwatch logs to check if provisioned or serverless will be a better for your workload. 

### Minimum IAM policies required
* AmazonRDSReadOnlyAccess
* AWSPriceListServiceFullAccess


### How to run the script
* Configure AWS Cli [https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html]
* Install python3.7 or above 
* install pip3
* Clone this repository to the directory
* Install required libraries 


```
pip3 install requirements.txt
```


Parameter names:
| Parameter        | Details          | Default  | 
| ------------- |:-------------:| -----:| -----: |
| -n, --name      | Neptune instance name |  |
| -r, --region     | Region name for instance      |    |
| -p, --period | Number of days datapoints collected from cloudwatch      |    14 |


Example call to evaluate if current provisioned workload will be cheaper on serverless:
```
python pricing.py -n database-1 -p 1 -r us-east-1

Output:
Region : us-east-1
Instance name : database-1
Instance type : db.t3.medium
Data collection period : 1 days
Cost of running on-demand provisioned instance : $0.098/hr
Total cost of running provisioned for 1 days : $2.31
Total cost of running serverless for 1 days : $3.79
Maximum NCU utilization : 1
Minimum NCU utilization : 1
Average NCU utilization : 1
Total data points : 1415
```

Another Example of checking if OnDemand provisioned instances are cheaper than running Neptune serverless:

```
python pricing.py -n database-2-instance-1 -p 1 -r us-east-1

Region : us-east-1
Instance name : database-2-instance-1
Instance type : db.serverless
Data collection period : 1 days
Total cost of running serverless for last 1 days : $4.1
Minimum NCU utilization : 1.0 ,Equivalent OnDemand Instance Costs: (db.r6g.large) $6.903
Maximum NCU utilization : 5.0 ,Equivalent OnDemand Instance Costs: (db.r6g.xlarge) $13.805
90th Percentile NCU Utilization : 1.5 ,Equivalent OnDemand Instance Costs: (db.r6g.large) $6.903
Average NCU utilization : 1 ,Equivalent OnDemand Instance Costs: (db.r6g.large) $6.903
Total data points : 1208
```

