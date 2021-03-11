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

import argparse
import json
import traceback
import sys
from awsglue.job import Job

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


def _call_site(sc, call_site, info):
    return sc._jvm.CallSite(call_site, info)


def _as_java_list(sc, scala_seq_obj):
    return sc._jvm.GluePythonUtils.seqAsJava(scala_seq_obj)


def _as_scala_option(sc, some_val):
    return sc._jvm.GluePythonUtils.constructOption(some_val)


def _as_resolve_choiceOption(sc, choice_option_str):
    return sc._jvm.GluePythonUtils.constructChoiceOption(choice_option_str)


def callsite():
    return "".join(traceback.format_list(traceback.extract_stack()[:-2]))


# Definitions for Python 2/Python 3
if sys.version >= "3":
    def iteritems(d, **kwargs):
        return iter(d.items(**kwargs))
    def iterkeys(d, **kwargs):
        return iter(d.values(**kwargs))
    def itervalues(d, **kwargs):
        return iter(d.values(**kwargs))
else:
    def iteritems(d, **kwargs):
        return d.iteritems(**kwargs)
    def iterkeys(d, **kwargs):
        return d.iterkeys(**kwargs)
    def itervalues(d, **kwargs):
        return d.itervalues(**kwargs)

class GlueArgumentError(Exception):
    pass


# Define a custom argument parser that raises an exception rather than calling
# sys.exit() so that we can surface the errors.
class GlueArgumentParser(argparse.ArgumentParser):
    def error(self, msg):
        raise GlueArgumentError(msg)


def getResolvedOptions(args, options):
    parser = GlueArgumentParser()

    if Job.continuation_options()[0][2:] in options:
        raise RuntimeError("Using reserved arguments " + Job.continuation_options()[0][2:])

    if Job.job_bookmark_options()[0][2:] in options:
        raise RuntimeError("Using reserved arguments " + Job.job_bookmark_options()[0][2:])

    parser.add_argument(Job.job_bookmark_options()[0], choices =Job.job_bookmark_options()[1:], required = False)
    parser.add_argument(Job.continuation_options()[0], choices =Job.continuation_options()[1:], required = False)

    for option in Job.job_bookmark_range_options():
        if option[2:] in options:
            raise RuntimeError("Using reserved arguments " + option)
        parser.add_argument(option, required=False)

    for option in Job.id_params()[1:]:
        if option in options:
            raise RuntimeError("Using reserved arguments " + option)
        # TODO: Make these mandatory, for now for backward compatability making these optional, also not including JOB_NAME in the reserved parameters list.
        parser.add_argument(option, required=False)

    if Job.encryption_type_options()[0] in options:
        raise RuntimeError("Using reserved arguments " + Job.encryption_type_options()[0])
    parser.add_argument(Job.encryption_type_options()[0], choices = Job.encryption_type_options()[1:])

    if Job.data_lineage_options()[0] in options:
        raise RuntimeError("Using reserved arguments " + Job.data_lineage_options()[0])
    parser.add_argument(Job.data_lineage_options()[0], required=False)
        
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
    absent_range_option = []
    for option in Job.job_bookmark_range_options():
       key = option[2:].replace('-','_')
       if key not in parsed_dict or parsed_dict[key] is None:
           absent_range_option.append(option)
    if parsed_dict['job_bookmark_option']  == 'job-bookmark-pause':
        if len(absent_range_option) == 1:
            raise RuntimeError("Missing option or value for "  +  absent_range_option[0])
    else:
        if len(absent_range_option) == 0:
            raise RuntimeError("Invalid option(s)"  +  ' '.join(Job.job_bookmark_range_options()))

    _global_args.update(parsed_dict)

    return parsed_dict
