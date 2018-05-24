# bash file for submitting the job.

# App configuration, please update accordingly.
SPARK_PATH=/opt/spark-2.2.0-bin-hadoop2.7/bin/spark-submit
JAR_PATH=target/scala-2.11/fastrulemining_2.11-1.0.jar
MAIN_CLASS=Main
NCORES=4
DRIVER_MEMORY=8G
EXECUTOR_MEMORY=8G

logfile=fastRuleMining.log

facts_path=data/YAGO/YAGOFacts.csv
schema_path=data/YAGO/YAGOSchema.csv
rules_path=data/YAGO/YAGORules.csv-%d
output=output

function run() {
  ${SPARK_PATH} \
      --class ${MAIN_CLASS} \
      --master local[${NCORES}] \
      --driver-memory ${DRIVER_MEMORY} \
      --executor-memory ${EXECUTOR_MEMORY} \
      ${JAR_PATH} \
      ${facts_path} \
      ${schema_path} \
      ${rules_path} \
      ${output} \
      ${logfile} 
}

run
