# Copyright 2016-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
"""
   This script is supposed to be invoked by the tape-run.sh or PrepareLaunch class within the Tape container. It iterates
   the connections supplied to extract the ECR URL. Using the URL, the docker image will be downloaded in a per-layer 
   fashion and unpacked onto to the container file system. Finally, the paths to the connector jars are written out to an
   output file. Reference: https://rmannibucau.metawerx.net/post/docker-extracts-fileystem-with-bash
"""

import argparse
import gzip
import logging
import os
import random
import re
import shutil
import string
import subprocess
import sys
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urlparse
from os import path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from .connector_activation_util import boto_client_error

LAYER_TAR_DIR = "layers/tar"
LAYER_GZ_DIR = "layers/gz"
MARKETPLACE = "MARKETPLACE"
CUSTOM = "CUSTOM"
HTTP_PROXY = "HTTP_PROXY"
HTTPS_PROXY = "HTTPS_PROXY"
NO_PROXY = "NO_PROXY"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_stream_handler() -> None:
    """
    Add a new stream handler to the logger at module level to emit LogRecord to std.out. With this setup, logs will show
    up in both customer's logStream and our docker logStream to aid debugging.
    """
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - Glue ETL Marketplace - %(message)s")
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)


def run_commands(commands: List[str]) -> Tuple[bytes, bytes]:
    """
    Util function to run shell commands from Python.
    """
    process = subprocess.Popen(commands,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    logger.info(f"run_commands output - \"{' '.join(commands)}\"\n"
                f"stdout: {stdout.decode()}\n"
                f"stderr: {stderr.decode()}")
    return stdout, stderr


def send_get_request(url: str, header: Dict[str, str]) -> requests.Response:
    logger.debug(f"Sending GET request to {url} with {header.keys()} specified in header.")
    response = requests.get(url, headers=header)
    response.raise_for_status()
    return response


def parse_url(url: str) -> Tuple[str, str]:
    res = urlparse(url, allow_fragments=False)
    return res.netloc, res.path.strip("/")


def extract_ecr_region(ecr_root: str) -> Union[None, str]:
    """
    Extract AWS Region of the ECR registry from its root address
    e.g. xxxxxxxxxxxx.dkr.ecr.us-east-1.amazonaws.com
    """
    session = boto3.session.Session()
    for region in session.get_available_regions("ecr"):
        if region in ecr_root:
            return region


def extract_registry_id(ecr_root: str) -> str:
    """
    Extract AWS account id of the ECR registry from its root address
    e.g. xxxxxxxxxxxx.dkr.ecr.us-east-1.amazonaws.com
    """
    match = re.match(r"^(\d{12})\.dkr\.ecr\.[a-z]{2}-[a-z]{4}-\d\.amazonaws\.com$", ecr_root)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Invalid ECR url supplied, couldn't find aws account from {ecr_root}.")


@boto_client_error(logger)
def get_ecr_authorization_token(ecr_root: str) -> str:
    """
    Get the ECR authorization token to be used later to call ECR HTTP API. Even though not clearly documented, the
    region is actually required to get the correct token, otherwise ECR returns Code 400 when the wrong token is used.
    """
    region = extract_ecr_region(ecr_root)
    registry_id = extract_registry_id(ecr_root)
    ecr = boto3.client(service_name="ecr", region_name=region)
    logger.info(f"Requesting ECR authorization token for registryIds={registry_id} and region_name={region}.")
    response = ecr.get_authorization_token(registryIds=[registry_id])
    return response["authorizationData"][0]["authorizationToken"]


def parse_ecr_url(ecr_url: str) -> Tuple[str, str, str]:
    """
    Parse ECR root address, image name and tag from the given ECR URL.
    E.g. https://xxxxxxxxxxxx.dkr.ecr.us-east-1.amazonaws.com/salesforce:7.2.0-latest
    """
    ecr_root, repo = parse_url(ecr_url)
    if not re.match("^\d{12}\.dkr\.ecr\.[a-z]{2}-[a-z]{4}-\d\.amazonaws\.com$", ecr_root):
        raise ValueError("malformed registry, correct pattern is https://aws_account_id.dkr.ecr.region.amazonaws.com")
    if not re.match("^[^:]+:[^:]+$", repo):
        raise ValueError("malformed image name, only one colon allowed to delimit image name and tag")
    image_name, tag = repo.split(":")
    return ecr_root, image_name, tag


def get_docker_manifest(ecr_url: str, header: Dict[str, str]) -> Dict[str, Any]:
    """
    Returns the manifest for the given image in ECR. It includes information about an image such as layers, size and
    digest. We extract the layers to get the digest id to download archive file for each layer.
    """
    ecr_root, image_name, tag = parse_ecr_url(ecr_url)
    manifest_url = f"https://{ecr_root}/v2/{image_name}/manifests/{tag}"
    logger.info(f"Calling ECR HTTP API to get manifest of {ecr_url}.")
    manifest = send_get_request(manifest_url, header).json()
    return manifest


def download_and_unpack_docker_layer(ecr_url: str, digest: str, dir_prefix: str, header: Dict[str, str]) -> None:
    """
    Docker cli and the daemon process are both not available within Glue Python Shell runtime. In order to download
    docker image and extract the connector jars inside, we need to download the layers that consist the image and unpack
    the file system so that we can access the jar files. The layer itself has multiple levels of compression applied,
    which is why we need to download it as gz file and then unpack as tar file. The final unpack of the tar file is done
    via the 'tar' command line tool because the tarfile library doesn't work for permission issue.
    """
    logger.info(f"Download/unpacking {digest} layer of image: {ecr_url}.")
    layer_id = digest.split(":")[1]
    logger.info(f"Preparing layer url and gz file path to store layer {layer_id}.")
    layer_gz_path = f"{dir_prefix}/{LAYER_GZ_DIR}/{layer_id}.gz"
    ecr_root, image_name, tag = parse_ecr_url(ecr_url)
    layer_url = f"https://{ecr_root}/v2/{image_name}/blobs/{digest}"

    logger.info(f"Getting the layer file {layer_id} and store it as gz.")
    layer = send_get_request(layer_url, header)
    with open(layer_gz_path, "wb") as f:
        f.write(layer.content)

    logger.info(f"Unzipping the {layer_id} layer and store as tar file.")
    with gzip.open(f"{layer_gz_path}", "rb") as f_in:
        with open(f"{dir_prefix}/{LAYER_TAR_DIR}/{layer_id}", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    logger.info(f"Unarchiving {layer_id} layer as tar file.")
    run_commands(["tar", "-C", f"{dir_prefix}/{LAYER_TAR_DIR}/", "-xf", f"{dir_prefix}/{LAYER_TAR_DIR}/{layer_id}"])


def parse_args(args: List[str]) -> List[str]:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--connections",
                            required=True,
                            type=lambda x: x.split(","),
                            help="a list of connection names we'll use to download jars for")
    arg_parser.add_argument("--result_path",
                            required=True,
                            help="file path to store the jar downloading result")
    arg_parser.add_argument("--region",
                            required=True,
                            help="aws region of the connections supplied")
    arg_parser.add_argument("--endpoint",
                            required=True,
                            help="endpoint to use to talk with Glue service")
    arg_parser.add_argument("--proxy",
                            default=None,
                            help="proxy to talk to Glue backend in case of VPC job")
    parsed_args = arg_parser.parse_args(args)
    return [parsed_args.connections, parsed_args.result_path, parsed_args.region,
            parsed_args.endpoint, parsed_args.proxy]


def id_generator(size: int = 5, chars: str = string.ascii_uppercase + string.digits) -> str:
    """
    Generate a random Id using letters from "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" with {size} digits.
    """
    return ''.join(random.choice(chars) for _ in range(size))


def get_connection(region: str, endpoint: str, conn: str, proxy: str = None) -> Union[Dict, None]:
    """
    Get catalog connection metadata by calling Boto3 get_connection API, supports custom supplied region and endpoint.
    """
    config = Config()
    if proxy:
        config.proxies = {'https': proxy}
    glue = boto3.Session().client(
        service_name="glue",
        region_name=region,
        endpoint_url=endpoint,
        config=config
    )
    logger.info(f"using region: {region}, proxy: {proxy} and glue endpoint: {endpoint} to get connection: {conn}")
    try:
        return glue.get_connection(Name=conn)
    except ClientError:
        logger.exception(f"Failed to get connection detail for {conn}, skip jar downloading for it")
    except NoCredentialsError:
        logger.exception(f"Unable to get credential to call GetConnection for {conn}, skip jar downloading for it."
                         f" Check if the IAM role has the right permission or if you need to increase IMDS retry.")
    return None


def collect_files_by_suffix(input_dir: str, suffix: str) -> List[str]:
    """
    Given an input path to a directory, find all files ending with the input suffix. Return a list of absolute paths of
    these files.
    """
    res = []
    for dirpath, _, filenames in os.walk(input_dir):
        for file in filenames:
            if not file.endswith(suffix):
                continue
            else:
                abs_path = os.path.abspath(os.path.join(dirpath, file))
                res.append(abs_path)
    return res


@boto_client_error(logger, "Failed to download jars for custom connection from S3...")
def download_custom_jars(conn: Dict[str, Any], dest_folder: str = "/tmp/custom_connection_jars"):
    os.makedirs(dest_folder, exist_ok=True)
    s3_urls: List[str] = conn["Connection"]["ConnectionProperties"]["CONNECTOR_URL"].split(",")
    s3 = boto3.client("s3")
    res = []

    for url in s3_urls:
        if url.strip().startswith("s3://") and url.strip().endswith(".jar"):
            bucket, key = parse_url(url.strip())
            file_path = f"{dest_folder}/etl-{key.split('/')[-1]}"
            s3.download_file(bucket, key, file_path)
            res.append(file_path)
        else:
            logger.error("custom connection can only have S3 urls end with '.jar' as connector url.")
    logger.info(f"collected jar paths: {res} for connection: {conn}.")
    return res


def download_jars_per_connection(conn: str, region: str, endpoint: str, proxy: str = None) -> List[str]:
    # validate connection type
    connection = get_connection(region, endpoint, conn, proxy)
    if connection is None:
        return []
    # download jars from S3 in case of custom connection
    elif connection["Connection"]["ConnectionType"] == CUSTOM:
        logger.info(f"Connection {conn} is a Custom connection, try to download jars for it from S3.")
        return download_custom_jars(connection)
    # return empty list in case of non-marketplace connection
    elif connection["Connection"]["ConnectionType"] != MARKETPLACE:
        logger.warning(f"Connection {conn} is not a Marketplace connection, skip jar downloading for it")
        return []
        
    # get the connection classname
    if "CONNECTOR_CLASS_NAME" in connection["Connection"]["ConnectionProperties"]:
        driver_name = connection["Connection"]["ConnectionProperties"]["CONNECTOR_CLASS_NAME"]

    # get the the connection ecr url
    ecr_url = connection["Connection"]["ConnectionProperties"]["CONNECTOR_URL"]
    ecr_root, _, _ = parse_ecr_url(ecr_url)

    # download the jars
    token = get_ecr_authorization_token(ecr_root)
    http_header = {"Authorization": f"Basic {token}"}

    manifest = get_docker_manifest(ecr_url, http_header)

    # make directory for the jars of the given connection
    dir_prefix = id_generator()
    os.makedirs(f"{dir_prefix}/{LAYER_TAR_DIR}", exist_ok=True)
    os.makedirs(f"{dir_prefix}/{LAYER_GZ_DIR}", exist_ok=True)

    for layer in manifest["layers"]:
        download_and_unpack_docker_layer(ecr_url, layer["digest"], dir_prefix, http_header)

    # return the jar paths
    res = collect_files_by_suffix(f"{dir_prefix}/{LAYER_TAR_DIR}/jars", ".jar")
    logger.info(f"Container paths are: {res}")

    # Write OEM key to /tmp/glue-marketplace.conf
    oem_key_path = f"{dir_prefix}/{LAYER_TAR_DIR}/oem/oem.txt"
    if path.exists(oem_key_path):
        with open(oem_key_path, 'r') as oem_file:
            oem_key = oem_file.readline()
            oem_value = oem_file.readline()
        output = """marketplace_oem = {
        %s = {
            oem_key = %s        oem_value = %s
          }
        }\n""" % (driver_name, oem_key, oem_value)
        with open("/tmp/glue-marketplace.conf", 'a') as opened_file:
            opened_file.write(output)
        logger.info(f"OEM information is written.")

    if not res:
        logger.warning(f"found no connector jars from {ecr_url} provided by {conn}, please contact AWS support of"
                       f" the Connector product owner to debug the issue.")
    else:
        logger.info(f"collected jar paths: {res} for connection: {conn}")
    return res


def main():
    # in case of VPC, we directly update config with proxy for glue client. Hence here we unset the environmental values
    # to avoid clients for other AWS services to go through Glue's proxy. The unset is process local and will not affect
    # subsequent aws cli usage.
    if HTTP_PROXY in os.environ:
        del os.environ[HTTP_PROXY]
    if HTTPS_PROXY in os.environ:
        del os.environ[HTTPS_PROXY]
    if NO_PROXY in os.environ:
        del os.environ[NO_PROXY]

    connections, result_path, region, endpoint, proxy = parse_args(sys.argv[1:])
    add_stream_handler()

    res = []
    for conn in connections:
        logger.info(f"Start downloading connector jars for connection: {conn}")
        res += download_jars_per_connection(conn, region, endpoint, proxy)

    # concatenate the jar paths as a string and write it out to result_path
    with open(result_path, "w") as f:
        f.write(",".join(res))

    logger.info(f"successfully wrote jar paths to \"{result_path}\"")


if __name__ == "__main__":
    main()
