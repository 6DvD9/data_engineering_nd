import boto3
import configparser
import json
import pandas as pd
import psycopg2
import time

from botocore.exceptions import ClientError


class CreateDeleteMyCluster:
    """Class to create and/or delete EC2, S3, IAM and Redshift cluster"""
    
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    
    def create_clients(self, KEY, SECRET):
        """Creates clients for EC2, S3, IAM and Redshift"""
        
        ec2 = boto3.resource(
            'ec2',
            region_name='us-west-2',
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )

        s3 = boto3.resource(
            's3',
            region_name='us-west-2',
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )

        iam = boto3.client(
            'iam',
            region_name='us-west-2',
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )

        redshift = boto3.client(
            'redshift',
            region_name='us-west-2',
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        
        return ec2, s3, iam, redshift

    def create_iam_role(self, iam, DWH_IAM_ROLE_NAME):
        """
        Creates IAM role to make Redhist access S3 bucket (read-only)
        Returns: roleArn    
        """
        
        #1.1 Create the role, 
        try:
            print("1.1 Creating a new IAM Role \n") 
            dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)


        print("1.2 Attaching Policy \n")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']

        print("1.3 Get the IAM role ARN \n")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        
        return roleArn
    
    def create_redshift_cluster(self, redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, 
                                DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
        """Create a Redshift cluster"""
        try:
            response = redshift.create_cluster( 
                
                # Parameters for hardware
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),

                # Parameters for identifiers & credentials
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,

                # Parameter for role (to allow s3 access)
                IamRoles=[roleArn] 
            )
        except Exception as e:
            print(e)
            
    def prettyRedshiftProps(self, props):
        """Function to describe cluster and see it's status"""
        
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", 
                      "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in props.items() if k in keysToShow]
        
        return pd.DataFrame(data=x, columns=["Key", "Value"])
    
    def open_ports_access_cluster_endpoint(self, ec2, myClusterProps, DWH_PORT):
        """Open incoming TCP port to access cluster endpoint"""
        try:
            vpc = ec2.Vpc(id=myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)

            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name, 
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
            print("Cluster endpoint now available!")
            
        except Exception as e:
            print(e)
    
    def create_and_run_all(self):
        """Putting all the functions above together to create clients, clusters and open access"""
        ec2, s3, iam, redshift = self.create_clients(self.KEY, self.SECRET)
        
        role_arn = self.create_iam_role(iam, self.DWH_IAM_ROLE_NAME)
        
        self.create_redshift_cluster(redshift, self.DWH_CLUSTER_TYPE, self.DWH_NODE_TYPE, self.DWH_NUM_NODES, self.DWH_DB, 
                                self.DWH_CLUSTER_IDENTIFIER, self.DWH_DB_USER, self.DWH_DB_PASSWORD, roleArn=role_arn)
        
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = self.prettyRedshiftProps(myClusterProps)
        print(df.iloc[2]['Value'])
        
        while True:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            df = self.prettyRedshiftProps(myClusterProps)
            print(df)
            time.sleep(60)
            
            if df.iloc[2]['Value'] == 'available':
                break
        
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = self.prettyRedshiftProps(myClusterProps)
        print(df)
        print('\n')
        
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        print('\n')
        
        self.open_ports_access_cluster_endpoint(ec2, myClusterProps, self.DWH_PORT)
        
        conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={self.DWH_DB} user={self.DWH_DB_USER} \
                                password={self.DWH_DB_PASSWORD} port={self.DWH_PORT}")
        cur = conn.cursor()
        
        print('Connected')
        
        conn.close()
        
    def delete_cluster(self):
        """Deletes all clients and clusters"""
        ec2, s3, iam, redshift = self.create_clients(self.KEY, self.SECRET)
        
        redshift.delete_cluster(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df = self.prettyRedshiftProps(myClusterProps)
        print(df)
        print(df.iloc[2]['Value'])
        
        iam.detach_role_policy(RoleName=self.DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)
        
            
if __name__ == "__main__":
    create_del_cluster = CreateDeleteMyCluster()
    
    # Toggle switch by setting create or delete to 1:
    create = 1
    delete = 0
    
    if create:
        create_del_cluster.create_and_run_all()
        
    if delete:
        create_del_cluster.delete_cluster()

