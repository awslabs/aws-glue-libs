# Copyright 2016-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# Licensed under the Amazon Software License (the "License"). You may not use
# this file except in compliance with the License. A copy of the License is
# located at
#
#  http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.

import boto3
import os
import logging
import copy
from datetime import datetime


class ExecutionProperty:
    def __init__(self, maxConcurrentRuns=1):
        self.maxConcurrentRuns = maxConcurrentRuns

    def __repr__(self):
        return "{'maxConcurrentRuns': "+ str(self.maxConcurrentRuns)+ "}"

    def as_dict(self):
        return {'maxConcurrentRuns': self.maxConcurrentRuns}


class Command:
    def __init__(self, name, scriptLocation):
        self.name=name
        self.scriptLocation=scriptLocation

    def __repr__(self):
        return "{'name': '"+ str(self.name)+",' 'scriptLocation': '"+ str(self.scriptLocation)+"'}"

    def as_dict(self):
        return {'name': self.name, 'scriptLocation': self.scriptLocation}


class Connections:
    def __init__(self, connections=[]):
        self.connections=connections

    def __repr__(self):
        return "{'connections': "+str(self.connections) + "}"

    def as_dict(self):
        return {'connections': self.connections}


class Job:
    def __init__(self):
        self.name = ''
        self.description = ''
        self.logUri = ''
        self.role = ''
        self.executionProperty = ExecutionProperty()
        self.command = Command("glueetl", "UNKNOWN")
        self.defaultArguments = {}
        self.connections = Connections()
        self.maxRetries = 1
        self.allocatedCapacity = 1
        self.createdOn = datetime.now()
        self.lastModifiedOn = datetime.now()

    def __repr__(self):
        return "{'command': "+str(self.command) + ",\n" + \
            "'connections': "+str(self.connections) + ",\n" + \
            "'createdOn': "+str(self.createdOn) + ",\n" + \
            "'description': '"+str(self.description) + "',\n" + \
            "'defaultArguments': "+str(self.defaultArguments) + ",\n" + \
            "'executionProperty': "+str(self.executionProperty) + ",\n" + \
            "'lastModifiedOn': "+str(self.lastModifiedOn) + ",\n" + \
            "'logUri': '"+str(self.logUri) + "',\n" + \
            "'maxRetries': "+str(self.maxRetries) + ",\n" + \
            "'name': '"+str(self.name) + "',\n" + \
            "'role': '"+str(self.role) + "',\n" + \
            "}"

    def as_dict(self):
        job_dict = {}
        job_dict['command'] = self.command.as_dict()
        if len(self.connections.connections) > 0:
            job_dict['connections'] = self.connections.as_dict()
        job_dict['createdOn'] = self.createdOn
        if len(self.description) > 0:
            job_dict['description'] = self.description
        job_dict['defaultArguments'] = self.defaultArguments
        job_dict['executionProperty'] = self.executionProperty.as_dict()
        job_dict['lastModifiedOn'] = self.lastModifiedOn
        job_dict['logUri'] = self.logUri
        job_dict['maxRetries'] = self.maxRetries
        job_dict['name'] = self.name
        job_dict['role'] = self.role
        return job_dict

    def as_job_create_dict(self):
        job_dict = copy.deepcopy(self.as_dict())
        del job_dict['createdOn']
        del job_dict['lastModifiedOn']
        return job_dict

    def as_job_update_dict(self):
        job_dict = copy.deepcopy(self.as_dict())
        del job_dict['name']
        del job_dict['createdOn']
        del job_dict['lastModifiedOn']
        return job_dict


class GlueJobUtils:
    def __init__(self, glue_context):
        proxy_url = glue_context._jvm.AWSConnectionUtils.getGlueProxyUrl()
        glue_endpoint = glue_context._jvm.AWSConnectionUtils.getGlueEndpoint()
        region = glue_context._jvm.AWSConnectionUtils.getRegion()
        # s3 service calls are not allowed through the proxy for the moment, so we use the s3 vpc endpoint instead
        self.s3 = boto3.resource('s3')
        # Boto does not have a API to set proxy information. It uses environment variables to lookup proxy informtion
        if not proxy_url[8:].startswith('null'):
            os.environ['https_proxy'] = proxy_url
        self.glue = boto3.client('glue', endpoint_url=glue_endpoint, region_name=region)


    def _glue_job_response_to_job(self, response_job):
        job = Job()
        job.name = response_job['name']

        try:
            job.description = response_job['description']
        except KeyError:
            logging.warning('description is missing in job response for job %s' % job.name)

        try:
            job.defaultArguments = response_job['defaultArguments']
        except KeyError:
            logging.warning('defaultArguments is missing in job response for job %s' % job.name)

        try:
            job.logUri = response_job['logUri']
        except KeyError:
            logging.warning('logUri is missing in job response for job %s' % job.name)

        try:
            job.role = response_job['role']
        except KeyError:
            logging.warning('role is missing in job response for job %s' % job.name)

        try:
            execution_property_dict = response_job['executionProperty']
            job.executionProperty = ExecutionProperty(execution_property_dict['maxConcurrentRuns'])
        except KeyError:
            logging.warning('executionProperty is missing in job response for job %s' % job.name)

        try:
            command_dict = response_job['command']
            job.command = Command(command_dict['name'], command_dict['scriptLocation'])
        except KeyError:
            logging.warning('command is missing in job response for job %s' % job.name)

        try:
            connections_dict = response_job['connections']
            job.connections = Connections(connections_dict['connections'])
        except KeyError:
            logging.warning('connections is missing in job response for job %s' % job.name)

        try:
            job.maxRetries = response_job['maxRetries']
        except KeyError:
            logging.warning('maxRetries is missing in job response for job %s' % job.name)

        try:
            job.createdOn = response_job['createdOn']
        except KeyError:
            logging.warning('createdOn is missing in job response for job %s' % job.name)

        try:
            job.lastModifiedOn = response_job['lastModifiedOn']
        except KeyError:
            logging.warning('lastModifiedOn is missing in job response for job %s' % job.name)

        return job

    def get_jobs(self, nextToken=''):
        response = self.glue.get_jobs(nextToken=nextToken)
        list_jobs_response = {}
        try:
            list_jobs_response['NextToken'] = response['NextToken']
        except KeyError:
            logging.info('NextToken is not present in get_jobs response')
        list_jobs_response['jobs'] = [self._glue_job_response_to_job(j) for j in response['jobs']]
        return list_jobs_response

    def get_job(self, jobName):
        response = self.glue.get_job(jobName=jobName)
        return self._glue_job_response_to_job(response['job'])

    def _get_bucket_prefix_from_s3_url(self, s3_url):
        if not s3_url.startswith('s3://'):
            raise Exception('s3 url for scriptLocation should start with s3:// but given %s' % s3_url)
        url_parts = s3_url[5:].split('/', 1)
        if not len(url_parts) == 2:
            raise Exception('s3 url for scriptLocation does not include a prefix: %s' % s3_url)
        if url_parts[1].endswith('/'):
            raise Exception('s3 url for scriptLocation should ot end with '/': %s' % s3_url)
        return {'bucket': url_parts[0], 'prefix': url_parts[1]}

    def _upload_file_to_s3(self, s3_url, file):
        if len(file) == 0:
            logging.warning('script file is not specified, skipping upload of script to s3')
        else:
            s3_parts = self._get_bucket_prefix_from_s3_url(s3_url)
            self.s3.Object(s3_parts['bucket'], s3_parts['prefix']).put(Body=open(file, 'rb'))

    def create_job(self, job, file=''):
        try:
            self._upload_file_to_s3(job.command.scriptLocation, file)
            return self.glue.create_job(**job.as_job_create_dict())
        except Exception as inst:
            print inst
            logging.error('Failed to create job')

    def update_job(self, job, file=''):
        try:
            self._upload_file_to_s3(job.command.scriptLocation, file)
            return self.glue.update_job(jobName=job.name, jobUpdate=job.as_job_update_dict())
        except Exception as inst:
            print inst
            logging.error('Failed to update job')

    def delete_job(self, jobName):
        return self.glue.delete_job(jobName=jobName)

