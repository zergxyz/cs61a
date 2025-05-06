from pyspark.sql import SparkSession
import sparknlp 

print("Attempting to start Spark session manually...")

# Manual session creation is safer for offline use with --jars / --py-files
# It avoids sparknlp.start() attempting downloads.
spark = SparkSession.builder \
   .appName("SparkNLP_OSS_Hello_World") \
   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
   .config("spark.kryoserializer.buffer.max", "1000M") \
   .config("spark.jars.packages", "") \
   .config("spark.jars.ivy", "/tmp/.ivy2/") \
   .getOrCreate()
    #.config("spark.jars", "gs://<your-bucket>/dependencies/jars/spark-nlp-assembly-5.5.1.jar") # Redundant if using --jars, but can be explicit
   

print("Spark session created.")
print(f"Spark NLP version: {sparknlp.version()}")
print(f"Apache Spark version: {spark.version}")
print(f"Java version: {spark.sparkContext.getConf().get('spark.driver.extraJavaOptions', '')}") # Check Java version if needed

# Simple Spark NLP pipeline (DocumentAssembler + SentenceDetector)
# Avoids models requiring downloads for this basic test.
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import SentenceDetector
from pyspark.ml import Pipeline

try:
    documentAssembler = DocumentAssembler().setInputCol("text").setOutputCol("document")
    sentenceDetector = SentenceDetector().setInputCols(["document"]).setOutputCol("sentence")
    pipeline = Pipeline(stages=[documentAssembler, sentenceDetector])

    data = spark.createDataFrame([["hello, world"]]).toDF("text")
    result = pipeline.fit(data).transform(data)

    print("Pipeline worked! Sample output:")
    result.select("sentence.result").show(truncate=False)

except Exception as e:
    print(f"Error during Spark NLP pipeline execution: {e}")
    # Add more detailed error logging if necessary

finally:
    print("Stopping Spark session.")
    spark.stop()


'''
#!/bin/bash
# Copy JAR to Spark jars directory
gsutil cp gs://my-bucket/spark-nlp/spark-nlp_2.12-6.0.0.jar $SPARK_HOME/jars/
# Copy wheel to /tmp
gsutil cp gs://my-bucket/spark-nlp/spark_nlp-6.0.0-py3-none-any.whl /tmp/
# Install the wheel
pip install /tmp/spark_nlp-6.0.0-py3-none-any.whl


gcloud dataproc clusters create my-cluster \
--region=us-central1 \
--zone=us-central1-a \
--image-version=2.0-debian10 \
--master-machine-type=n1-standard-4 \
--worker-machine-type=n1-standard-4 \
--num-workers=2 \
--initialization-actions=gs://my-offline-packages/scripts/install_sparknlp.sh
'''
