from os import environ, walk
from os.path import relpath, join, abspath, dirname
from typing import IO
from boto3.s3.transfer import TransferConfig
from mypy_boto3_mwaa.client import MWAAClient
from mypy_boto3_s3.client import S3Client
from zipfile import ZipFile
import logging
import asyncio
import aioboto3  
import pathlib
import io

MWAA_ENVIRONMENT_NAME=environ.get('MWAA_ENVIRONMENT_NAME')
AWS_S3_BUCKET=environ.get('AWS_S3_BUCKET')
AWS_ACCESS_KEY_ID=environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION=environ.get('AWS_REGION', 'us-east-1')
AWS_S3_ENDPOINT=environ.get('AWS_S3_ENDPOINT')
AWS_DEST_DIR=environ.get('AWS_S3_DEST_DIR')
ROOT_DIR = dirname(abspath(__file__))
AIRFLOW_FOLDER=environ.get('AIRFLOW_FOLDER')
AIRFLOW_DIR=join(ROOT_DIR, AIRFLOW_FOLDER)

PLUGINS_DIR=join(AIRFLOW_DIR, 'plugins')
AWS_S3_PLUGINS_DIR = join(AWS_DEST_DIR, 'plugins.zip')

DAGS_DIR = join(AIRFLOW_DIR, 'dags')
AWS_S3_DAG_FOLDER=join(AWS_DEST_DIR, 'dags')

REQUIREMENTS_DIR = join(AIRFLOW_DIR, 'requirements.txt')
AWS_S3_REQUIREMENTS_DIR = join(AWS_DEST_DIR, 'requirements.txt') 

PluginsS3ObjectVersion = None
RequirementsS3ObjectVersion = None

plugins_whitelist = ['.py']
dags_whitelist = ['.py']

KB = 1024
MB = KB ** 2
GB = KB ** 3

if not AWS_S3_BUCKET:
    raise ValueError("AWS_S3_BUCKET is not set. Quitting.") 

if not AWS_ACCESS_KEY_ID:
    raise ValueError("AWS_ACCESS_KEY_ID is not set. Quitting.") 

if not AWS_SECRET_ACCESS_KEY:
    raise ValueError("AWS_SECRET_ACCESS_KEY is not set. Quitting.") 

if not AWS_S3_BUCKET:
    raise ValueError("AWS_S3_BUCKET is not set. Quitting.") 

if AWS_S3_ENDPOINT:
    ENDPOINT_APPEND=f"--endpoint-url {AWS_S3_ENDPOINT}"

creds = dict(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION)
  
async def requirements(s3, config):    
    await s3.upload_file(
                Filename=REQUIREMENTS_DIR, 
                Bucket=AWS_S3_BUCKET, 
                Key=AWS_S3_REQUIREMENTS_DIR,
                Config=config)     
    return await s3.head_object(Bucket=AWS_S3_BUCKET, Key=AWS_S3_REQUIREMENTS_DIR)
            
async def plugins(s3, config, plugins_whitelist):    
    zip_stream:IO[bytes]=io.BytesIO()
    with ZipFile(zip_stream, 'w') as zip:
        for source, dirs, files in walk(PLUGINS_DIR):
            for filename in files:
                if(pathlib.Path(filename).suffix not in plugins_whitelist): continue
                filepath = join(source, filename)
                zip.write(filename=filepath, arcname=join(relpath(source,PLUGINS_DIR),filename))
    zip_stream.seek(0)
    await s3.upload_fileobj(Fileobj=zip_stream, Bucket=AWS_S3_BUCKET, Key=AWS_S3_PLUGINS_DIR, Config=config)
    return await s3.head_object(Bucket=AWS_S3_BUCKET, Key=AWS_S3_PLUGINS_DIR)

async def dags(s3, config, dags_whitelist):    
    for source, dirs, files in walk(DAGS_DIR):
        for filename in files:
            if(pathlib.Path(filename).suffix not in dags_whitelist): continue
            # construct the full local path
            local_file = join(source, filename)
            # construct the full Dropbox path
            relative_path = relpath(local_file, DAGS_DIR)
            s3_file = join(AWS_S3_DAG_FOLDER, relative_path)
            # Invoke upload function
            await s3.upload_file(
                Filename=local_file, 
                Bucket=AWS_S3_BUCKET, 
                Key=s3_file,
                Config=config)

async def update_environment(mwaa, plugins_object_version, s3_object_version):    
   
        return await mwaa.update_environment(
            Name=MWAA_ENVIRONMENT_NAME,
            DagS3Path=AWS_S3_DAG_FOLDER, 
            PluginsS3Path=AWS_S3_PLUGINS_DIR,
            PluginsS3ObjectVersion=plugins_object_version,
            RequirementsS3ObjectVersion=s3_object_version,
            RequirementsS3Path=AWS_S3_REQUIREMENTS_DIR,
            SourceBucketArn=f"arn:aws:s3:::{AWS_S3_BUCKET}",
        )

async def execute():
    config = TransferConfig(multipart_threshold= 5 * GB)
    s3: S3Client
    async with aioboto3.Session().client("s3", **creds) as s3:
        #upload requirements.txt file        
        response=await requirements(s3=s3, config=config)
        s3_object_version=response.get('VersionId')    
        #zip plugins and upload file             
        response=await plugins(s3=s3, config=config, plugins_whitelist=plugins_whitelist)
        plugins_object_version=response.get('VersionId')    
        #upload dags folder
        await dags(s3=s3, config=config, dags_whitelist=dags_whitelist)
    mwaa: MWAAClient
    async with aioboto3.Session().client("mwaa", **creds) as mwaa: 
        await update_environment(
            mwaa=mwaa, 
            s3_object_version=s3_object_version, 
            plugins_object_version=plugins_object_version)
          
if __name__ == "__main__":
    try:
        asynchrony: asyncio.run(execute())
    except Exception as e:
        logging.exception('deployment failed')
        raise e