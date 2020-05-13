REM The code assumes you have JAVA_HOME and HADOOP_HOME variables are set according to the instructions in the README
echo off
SET ORIGINAL_PYTHON_PATH=%PYTHONPATH%

FOR /F "tokens=* USEBACKQ" %%F IN (`cd`) DO (
SET ROOT_DIR=%%F
)


SET SPARK_CONF_DIR=%ROOT_DIR%\conf
SET GLUE_JARS_DIR=%ROOT_DIR%/jarsv1

SET PYTHONPATH=%SPARK_HOME%\python\;%PYTHONPATH%
for %%x in (%SPARK_HOME%\python\lib\py4j-*-src.zip) do (
    SET PYTHONPATH=%PYTHONPATH%;%%x
)

REM Generate the zip archive for glue python modules
REM If you did not have powershell available then replace the following step with static zip file
del PyGlue.zip
powershell Compress-Archive awsglue PyGlue.zip
REM Asssuming PyGlue.zip file is available

SET GLUE_PY_FILES=%ROOT_DIR%\PyGlue.zip
SET PYTHONPATH=%GLUE_PY_FILES%;%PYTHONPATH%

REM Run mvn copy-dependencies target to get the Glue dependencies locally
SET RETURN = mvn -f %ROOT_DIR%/pom.xml -DoutputDirectory=%ROOT_DIR%/jarsv1 dependency:copy-dependencies
SET SPARK_CONF_DIR=%ROOT_DIR%\conf
mkdir %SPARK_CONF_DIR%
del %SPARK_CONF_DIR%\spark-defaults.conf
REM Generate spark-defaults.conf
SET GLUE_JARS_DIR=%GLUE_JARS_DIR:\=/%
echo spark.driver.extraClassPath    %GLUE_JARS_DIR%/* >> %SPARK_CONF_DIR%\spark-defaults.conf
echo spark.executor.extraClassPath  %GLUE_JARS_DIR%/* >> %SPARK_CONF_DIR%\spark-defaults.conf

REM calling the pyspark and forwarding the arguments

%SPARK_HOME%\bin\spark-submit --py-files %GLUE_PY_FILES% %*
SET PYTHONPATH=%ORIGINAL_PYTHON_PATH%

