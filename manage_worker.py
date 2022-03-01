import boto3
import paramiko
from datetime import datetime
import time
import os

# EC2 access credentials
ec2_client = boto3.client('ec2')
ec2_resource = boto3.resource('ec2')

# global paramiko initialization
ssh=paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# instance id
instance_ids = ['i-01be23ac6a1572544']

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

            try:
                ssh.connect(hostname, username='ubuntu', key_filename='/home/ubuntu/.ssh/main-key.pem')
                print(f'Validated a connection to the host, moving on to code execution')
                ssh.close()
                return hostname
            except paramiko.ssh_exception.NoValidConnectionsError:
                print(f'Unable to establish connection to host...waiting another 30 secs')
                continue
        else:
            continue

    raise ValueError("Instance not started in 240 seconds")

def run_job(hostname):

    """
    Execute the docker build / run commands on the newly started instance

    :param hostname: the hostname of the started instance (although the instance id is constant, a new hostname is assigned on every re-start)
    """
    print(f'In run job')
    print(f'Connecting to {hostname}')
    ssh.connect(hostname, username='ubuntu', key_filename='/home/ubuntu/.ssh/main-key.pem')
        
    stdin, stdout, stderr=ssh.exec_command('cd /home/ubuntu/flippr; docker build -t flippr .; docker run -it --env-file .env --log-driver=awslogs --log-opt awslogs-region=us-east-1 --log-opt awslogs-group=flippr --rm flippr:latest', get_pty=True)
    # print(stdout.readlines())
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


def get_hostname(instance_ids):

    """
    function to get the hostname of an instance

    :param instance_ids: the ids of the instances to get the hostnames of

    """

    hostname_response=ec2_resource.meta.client.describe_instances(InstanceIds=instance_ids)['Reservations'][0]['Instances']
    hostname=hostname_response[0]['PublicDnsName']
    print(f'Hostname is {hostname}')
    return hostname

if __name__ == '__main__':
    # Check ETL main instance status
    status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
    hostname=''
    if not status_response:
        hostname=start_instance(instance_ids)
        print('The main ETL instance was stopped at ' + str(datetime.now()))
    else:
        hostname=get_hostname(instance_ids)
        print('The main ETL instance was running at the start of the job')

    run_job(hostname)
    # stop_instance(instance_ids)
