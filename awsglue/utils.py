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

import argparse
import json
import traceback
import sys
from job import Job

_global_args = {}

def makeOptions(sc, py_obj):
    if isinstance(py_obj, dict):
        json_string = json.dumps(py_obj)
    elif isinstance(py_obj, basestring):
        json_string = py_obj
    else:
        raise TypeError("Unexpected type " + str(type(py_obj))
                        + " in makeOptions")
    return sc._jvm.JsonOptions(json_string)

def callsite():
    return "".join(traceback.format_list(traceback.extract_stack()[:-2]))


def getResolvedOptions(args, options):
    parser = argparse.ArgumentParser()

    if Job.continuation_options()[0][2:] in options:
        raise RuntimeError("Using reserved arguments " + Job.continuation_options()[0][2:])

    if Job.job_bookmark_options()[0][2:] in options:
        raise RuntimeError("Using reserved arguments " + Job.job_bookmark_options()[0][2:])

    parser.add_argument(Job.job_bookmark_options()[0], choices =Job.job_bookmark_options()[1:], required = False)
    parser.add_argument(Job.continuation_options()[0], choices =Job.continuation_options()[1:], required = False)

    for option in Job.id_params()[1:]:
        if option in options:
            raise RuntimeError("Using reserved arguments " + option)
        # TODO: Make these mandatory, for now for backward compatability making these optional, also not including JOB_NAME in the reserved parameters list.
        parser.add_argument(option, required=False)

    if Job.encryption_type_options()[0] in options:
        raise RuntimeError("Using reserved arguments " + Job.encryption_type_options()[0])
    parser.add_argument(Job.encryption_type_options()[0], choices = Job.encryption_type_options()[1:])
        
    # TODO: Remove special handling for 'RedshiftTempDir' and 'TempDir' after TempDir is made mandatory for all Jobs
    # Remove 'RedshiftTempDir' and 'TempDir' from list of user supplied options
    options = [opt for opt in options if opt not in {'RedshiftTempDir', 'TempDir'}]
    parser.add_argument('--RedshiftTempDir', required=False)
    parser.add_argument('--TempDir', required=False)

    for option in options:
        parser.add_argument('--' + option, required=True)

    parsed, extra = parser.parse_known_args(args[1:])

    parsed_dict = vars(parsed)

    # TODO: remove special handling after TempDir is made mandatory for all jobs
    if 'TempDir' in parsed_dict and parsed_dict['TempDir'] is not None:
        # TODO: Remove special handling for 'RedshiftTempDir' and 'TempDir'
        parsed_dict['RedshiftTempDir'] = parsed_dict['TempDir']
    elif 'RedshiftTempDir' in parsed and parsed_dict['RedshiftTempDir'] is not None:
        parsed_dict['TempDir'] = parsed_dict['RedshiftTempDir']

    # Special handling for continuations. If --job-bookmark-option is set we
    # use that, regardless of whether --continuation-option is set. If
    # --job-bookmark-option is not set but --continuation-option is set, fall
    # back to that.

    bookmark_value = parsed_dict.pop("continuation_option", None)
    if 'job_bookmark_option' not in parsed_dict or parsed_dict['job_bookmark_option'] is None:
        if bookmark_value is None:
            bookmark_value = Job.job_bookmark_options()[3]
        else:
            # translate old style continuation options into job-bookmark options
            option_index = Job.continuation_options().index(bookmark_value)
            bookmark_value = Job.job_bookmark_options()[option_index]

        parsed_dict['job_bookmark_option'] = bookmark_value

    _global_args.update(parsed_dict)

    print 'Found extra arguments', extra

    return parsed_dict
