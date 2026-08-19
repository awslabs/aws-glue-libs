---
name: venv-migration
description: Migrate an AWS Glue job from --additional-python-modules to --python-virtual-env.
---

## Execution instructions

When the user asks you to migrate a Glue job from `--additional-python-modules` to
`--python-virtual-env`, execute the entire migration end to end using your tools. Do not
only describe the steps.

The only thing you should ask the user for is the S3 bucket and path to upload the final
tarball to (step 6). Execute everything else yourself.

Create all files in a working directory at `~/glue-venv-migration/`, build and run Docker
containers, and produce the final `pyspark_venv.tar.gz`.

## Background

Migrating from `--additional-python-modules` to `--python-virtual-env` requires a
requirements file that captures every dependency the job needs. This is not always
straightforward, because dependencies come from several sources and not all of them are
explicitly declared:

1. Explicitly named packages: packages passed directly to `--additional-python-modules`,
   for example `pandas==2.0,requests==2.28`.
2. Transitive dependencies: packages that pip installs to satisfy the dependencies of
   category 1. The customer might not know what these are, but pip resolves them
   automatically when building the venv.
3. Base container libraries: packages included in the Glue container image, for example
   numpy, boto3, and pandas. These were always available without being declared, so
   customer scripts might import them without ever specifying them as a dependency. Two
   factors complicate this:
   a. If a base container library was a transitive dependency of a package in
      `--additional-python-modules`, the customer might be unaware of the dependency.
   b. The version actually running might differ from the version listed in the Glue
      documentation, because `--additional-python-modules` can override the container
      version.
4. Requirements files in Amazon S3: the customer uses
   `--python-modules-installer-option: -r` with a requirements file in S3. The file might
   contain unpinned versions or version ranges, so the venv build could resolve to
   different versions than what previously ran on Glue.
5. Private pip index packages: the customer uses
   `--python-modules-installer-option: --no-deps --index-url` to pull from a self-hosted
   repository. The venv build environment must be able to reach the same private index.

## Environment by Glue version

| Glue version | Python | Base image | `base-requirements.txt` branch | Glue library |
|---|---|---|---|---|
| 5.0 | 3.11 | `public.ecr.aws/amazonlinux/amazonlinux:2023-minimal` | `glue-5.0` | `AWSGlueDataplanePython==5.0.0` |
| 5.1 | 3.11 | `public.ecr.aws/amazonlinux/amazonlinux:2023-minimal` | `glue-5.1` | `AWSGlueDataplanePython==5.1.0` |
| 6.0 | 3.13 | `public.ecr.aws/amazonlinux/amazonlinux:2023-minimal` | `main` | `AWSGlueDataplanePython==6.0.0` |

Substitute the Python version, branch, and library version from this table wherever the
steps below use `3.11`, `glue-5.0`, or `AWSGlueDataplanePython==5.0.0`.

## Steps to execute

Execute all of the following steps yourself. Do not ask the user to run commands.

### Step 1: Gather inputs from the user's request

From the user's message, extract:

- The Glue version, for example 5.0, 5.1, or 6.0
- The value of `--additional-python-modules`, for example "ephem, awscli"
- The Glue job script

### Step 2: Fetch the base container module list

Download `base-requirements.txt` for the user's Glue version from the branch named in the
table above. For example, for Glue 6.0:

```bash
curl -fsSL -o ~/glue-venv-migration/base-requirements.txt \
  https://raw.githubusercontent.com/awslabs/aws-glue-libs/main/base-requirements.txt
```

If that file is unavailable, fall back to the markdown version of the documentation page:
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.md

That endpoint contains the module lists for all Glue versions in plain text. From the
fetched content, extract the module list under the heading that matches the user's Glue
version, for example "AWS Glue version 5.0".

### Step 3: Create all files

Create the following files in `~/glue-venv-migration/`:

1. `base-requirements.txt` - the module list from step 2 for the user's Glue version.
2. `additional-requirements.txt` - one package per line from the user's
   `--additional-python-modules`.
3. `my_glue_script/main.py` - the user's Glue job script, so that pipreqs can analyze its
   imports.
4. `Dockerfile` - use the base image and Python version for the user's Glue version. For
   Glue 5.0 and 5.1:

   ```dockerfile
   FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2023-minimal

   RUN dnf install -y python3.11 zip && \
       dnf clean all

   WORKDIR /build
   ```

   For Glue 6.0, install `python3.13` instead of `python3.11`.

5. `build-venv.sh` - the build script that runs inside the container, described in step 5.
   In `build-venv.sh`, pin `AWSGlueDataplanePython` to the version that matches the user's
   Glue version, as listed in the table above.

### Step 4: Build the Docker image

```bash
cd ~/glue-venv-migration && docker build --platform linux/amd64 -t glue-venv-builder .
```

### Step 5: Run the build script inside Docker

Run the container non-interactively. Do not use `-it`, so that the container runs to
completion:

```bash
cd ~/glue-venv-migration && docker run --platform linux/amd64 \
  -v $(pwd)/base-requirements.txt:/working_dir/base-requirements.txt:ro \
  -v $(pwd)/additional-requirements.txt:/working_dir/additional-requirements.txt:ro \
  -v $(pwd)/my_glue_script/:/working_dir/my_glue_script/:ro \
  -v $(pwd)/build-venv.sh:/working_dir/build-venv.sh:ro \
  -v $(pwd):/output \
  -w /working_dir \
  glue-venv-builder bash build-venv.sh 2>&1
```

The `build-venv.sh` script must do the following. Adjust the Python version and the
`AWSGlueDataplanePython` version to match the user's Glue version.

```bash
#!/bin/bash
set -e

python3.11 -m venv temp_venv
source temp_venv/bin/activate
python3.11 -m pip install --upgrade pip
python3.11 -m pip install -r base-requirements.txt
python3.11 -m pip install -r additional-requirements.txt
pip freeze > full-requirements.txt

python3.11 -m pip install pipreqs pip-tools
pipreqs --mode no-pin --savepath discovered-requirements.txt /working_dir/my_glue_script
sed -i '/pyspark/d' discovered-requirements.txt
sed -i '/py4j/d' discovered-requirements.txt
sed -i '/awsglue/d' discovered-requirements.txt
pip-compile discovered-requirements.txt -c full-requirements.txt -o final-requirements.txt

echo "=== Final requirements.txt ==="
cat final-requirements.txt

deactivate
rm -rf temp_venv

python3.11 -m venv pyspark_venv
source pyspark_venv/bin/activate
python3.11 -m pip install --upgrade pip
python3.11 -m pip install -r final-requirements.txt
python3.11 -m pip install AWSGlueDataplanePython==5.0.0
python3.11 -m pip install venv-pack
venv-pack -f -o pyspark_venv.tar.gz
cp pyspark_venv.tar.gz /output/

echo "=== Done. pyspark_venv.tar.gz created ==="
```

### Step 6: Ask the user for the S3 destination and upload

After the tarball is built, ask the user for their S3 bucket and path. Then run:

```bash
aws s3 cp ~/glue-venv-migration/pyspark_venv.tar.gz s3://BUCKET/PATH/pyspark_venv.tar.gz
```

### Step 7: Show the user the updated job parameters

Tell the user to update their Glue job.

Remove:

```json
"--additional-python-modules": "<their old value>"
```

Add:

```json
"--python-virtual-env": "s3://BUCKET/PATH/pyspark_venv.tar.gz"
```

## Reference

- [Using Python virtual environments with AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)
- [Kiro](https://kiro.dev)
