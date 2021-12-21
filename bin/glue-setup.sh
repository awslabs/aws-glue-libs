#!/usr/bin/env bash

ROOT_DIR="$(cd $(dirname "$0")/..; pwd)"
cd $ROOT_DIR

SPARK_CONF_DIR=$ROOT_DIR/conf
GLUE_JARS_DIR=$ROOT_DIR/jarsv1

PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
PYTHONPATH=`ls $SPARK_HOME/python/lib/py4j-*-src.zip`:"$PYTHONPATH"

# Generate the zip archive for glue python modules
rm PyGlue.zip
zip -r PyGlue.zip awsglue
GLUE_PY_FILES="$ROOT_DIR/PyGlue.zip"
export PYTHONPATH="$GLUE_PY_FILES:$PYTHONPATH"

# Run mvn copy-dependencies target to get the Glue dependencies locally
mvn -f $ROOT_DIR/pom.xml -DoutputDirectory=$ROOT_DIR/jarsv1 dependency:copy-dependencies

export SPARK_CONF_DIR=${ROOT_DIR}/conf
mkdir $SPARK_CONF_DIR
rm $SPARK_CONF_DIR/spark-defaults.conf
# Generate spark-defaults.conf
echo "spark.driver.extraClassPath $SPARK_HOME/jars/*:$GLUE_JARS_DIR/*" >> $SPARK_CONF_DIR/spark-defaults.conf
echo "spark.executor.extraClassPath $SPARK_HOME/jars/*:$GLUE_JARS_DIR/*" >> $SPARK_CONF_DIR/spark-defaults.conf

# Restore present working directory
cd -
