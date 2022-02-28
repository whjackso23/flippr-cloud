import boto3
import paramiko
from datetime import datetime
import time
import os

# EC2 access credentials
ec2_client = boto3.client('ec2')
ec2_resource = boto3.resource('ec2')

# instance id
instance_ids = ['i-0342b56c2b3d3d6d1']

# Check ETL main instance status
status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
if not status_response:
    print('The main ETL instance was stopped at ' + str(datetime.now()))
else:
    print('The main ETL instance was running at the start of the job')

def start_instance(instance_ids):
    
    """
    Start the instance that runs the docker image, checking that it is online before returning the hostname

    :param instace_ids: the id of the instance to start
    """
    ec2_client.start_instances(InstanceIds=instance_ids, DryRun=False)
    for x in range(0, 12):
        time.sleep(30)
        status_response=ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
        if len(status_response)!=0 and status_response[0]['InstanceState']['Name']=='running':
            hostname_response=ec2_resource.meta.client.describe_instances(InstanceIds=instance_ids)['Reservations'][0]['Instances']
            hostname=hostname_response[0]['PublicDnsName']
            print(f'Hostname is {hostname}')
            return hostname
        else:
            continue

    raise ValueError("Instance not started in 240 seconds")

def run_job(hostname):

    """
    Execute the docker build / run commands on the newly started instance

    :param hostname: the hostname of the started instance (although the instance id is constant, a new hostname is assigned on every re-start)
    """
    print(f'In run job')
    ssh=paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname, username='ubuntu', key_filename='/home/ubuntu/.ssh/main-key.pem')
        
    stdin, stdout, stderr=ssh.exec_command('cd /home/ubuntu/flippr; docker build -t flippr .; docker run -it --env-file .env --rm flippr', get_pty=True)
    print(stdout.readlines())
    ssh.close()

def stop_instance(instance_ids):

    """
    Stop the instance

    :param instance_ids: the id of the instance
    """
    print(f'in stop instance')
    ec2_client.stop_instances(InstanceIds=instance_ids, DryRun=False)

    status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
    print(status_response)

hostname=start_instance(instance_ids)
run_job(hostname)
stop_instance(instance_ids)