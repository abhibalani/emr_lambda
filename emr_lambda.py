"""Lambda to launch EMR and start map reduce for an input"""
from types import SimpleNamespace

import boto3
import logging

DEBUG = False

logger = logging.getLogger()
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

BUCKET = 'test-bucket'

LOGS_PATH = 's3://{}/logs/emr_lambda/'.format(BUCKET)

INITIALIZATION_SCRIPT_PATH = 's3://{}/initialize.sh'.format(BUCKET)

MAPPER_FILE = 'mapper.py'
MAPPER_PATH = 's3://{}/code/{}'.format(BUCKET, MAPPER_FILE)

REDUCER_FILE = 'reducer.py'
REDUCER_PATH = 's3://{}/code/{}'.format(BUCKET, REDUCER_FILE)

INPUT_FILE = 'input.csv'
INPUT_PATH = 'data/{}'.format(INPUT_FILE)

OUTPUT_PATH = 'output'

KEY_PAIR = 'key-pair'  # An existing key pair name without extension

REGION_NAME = 'us-east-1'


def get_emr_client():
    """
    Method to create boto3 EMR client object
    :return: boto3 EMR client object
    """
    try:
        return boto3.client('emr', region_name=REGION_NAME)
    except Exception as e:
        logger.info(str(e))
        exit(0)


def lambda_handler(event, context):
    """

    :param event:
    :param context:
    :return:
    """

    try:

        get_emr_client().run_job_flow(
            Name='EMR Name',
            LogUri=LOGS_PATH,
            ReleaseLabel='emr-5.15.0',  # EMR version

            # Configuration for EMR cluster
            Instances={
                'InstanceGroups': [
                    {'Name': 'master',
                     'InstanceRole': 'MASTER',
                     'InstanceType': 'm3.xlarge',
                     'InstanceCount': 1,
                     },
                    {'Name': 'core',
                     'InstanceRole': 'CORE',
                     'InstanceType': 'm3.xlarge',
                     'InstanceCount': 2,
                     },

                ],
                'Ec2KeyName': KEY_PAIR  #This allows us to ssh with the keypair

                # Other available configurations for the instances

                # 'KeepJobFlowAliveWhenNoSteps': True,
                # 'EmrManagedSlaveSecurityGroup': 'sg-1234',
                # 'EmrManagedMasterSecurityGroup': 'sg-1234',
                # 'Ec2SubnetId': 'subnet-1q234',
            },

            # To install requirements in all nodes of EMR while it is setting up
            BootstrapActions=[
                {
                    'Name': 'Install packages',
                    'ScriptBootstrapAction': {
                        'Path': INITIALIZATION_SCRIPT_PATH
                    }
                }
            ],
            # Hadoop streaming command to start EMR
            Steps=[
                {'Name': 'Name of the Step',
                 'ActionOnFailure': 'TERMINATE_CLUSTER',
                 'HadoopJarStep': {
                     'Jar': 'command-runner.jar',
                     'Args': [
                         'hadoop-streaming',
                         '-files',
                         '{},{},{}'.format(MAPPER_PATH, REDUCER_PATH,
                                           INPUT_PATH),
                         '-mapper', MAPPER_FILE,
                         '-input', INPUT_PATH,
                         '-output', OUTPUT_PATH,
                         '-reducer', REDUCER_FILE
                     ]}
                 }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )
        return 1
    except Exception as e:
        logger.error(str(e))
        return 0, str(e)
