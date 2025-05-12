from pyspark.sql import SparkSession
import sparknlp
import sparknlp_jsl
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp_jsl.annotator import *
from pyspark.ml import Pipeline, PipelineModel

# Start SparkSession with JSL
# spark = sparknlp_jsl.start(SECRET) # Replace SECRET with your JSL secret

# 1. Load Data from BigQuery
# Assuming your DataFrame is loaded into 'bq_df' and has a column named 'text_column_name'
# Example:
# bq_df = spark.read.format("bigquery") \
# .option("table", "your_project.your_dataset.your_table") \
# .load()
#
# For demonstration, let's create a sample DataFrame:
data = [("Patient John Doe, age 45, visited Dr. Smith at General Hospital on 01/15/2023.",),
        ("Record for Jane Roe, 60 years old, seen by Dr. Emily White at City Clinic, MRN 12345.",)]
bq_df = spark.createDataFrame(data).toDF("text_column_name")

# 2. Build or Load De-identification Pipeline

# Option A: Using a Pretrained Clinical De-identification Pipeline
# Check John Snow Labs documentation for the latest available pretrained pipelines
# For example: pretrained_pipeline = PretrainedPipeline("clinical_deidentification", "en", "clinical/models")
#
# If using a pretrained pipeline directly with .transform(), it handles the internal stages.
# However, for customizing the DeIdentification annotator within it,
# you might need to unpack and repack or build a custom one.
# A more common approach for customization is building the pipeline manually:

# Option B: Building a Custom Pipeline
document_assembler = DocumentAssembler() \
    .setInputCol("text_column_name") \
    .setOutputCol("document")

sentence_detector = SentenceDetector() \
    .setInputCols(["document"]) \
    .setOutputCol("sentence")

tokenizer = Tokenizer() \
    .setInputCols(["sentence"]) \
    .setOutputCol("token")

word_embeddings = WordEmbeddingsModel.pretrained("embeddings_clinical", "en", "clinical/models") \
    .setInputCols(["sentence", "token"]) \
    .setOutputCol("embeddings")

# Use a clinical NER model suitable for de-identification
# (e.g., ner_deid_generic_augmented, ner_deid_subentity)
clinical_ner = MedicalNerModel.pretrained("ner_deid_generic_augmented", "en", "clinical/models") \
    .setInputCols(["sentence", "token", "embeddings"]) \
    .setOutputCol("ner")

ner_converter = NerConverterInternal() \
    .setInputCols(["sentence", "token", "ner"]) \
    .setOutputCol("ner_chunk")

# DeIdentification Annotator
# Configure for fixed character masking
de_identification = DeIdentification() \
    .setInputCols(["sentence", "token", "ner_chunk"]) \
    .setOutputCol("deidentified_text") \
    .setMode("mask") \
    .setMaskingPolicy("fixed_length_chars") \
    .setFixedMaskLength(4)  # Mask with 4 asterisks (****) for each entity. [1, 2]
    # You can also use .setSameLengthChars(True) to mask with the same number of asterisks as the original entity length.
    # And .setMaskingChars(['*']) to specify the character, though fixed_length_chars often defaults to asterisks.

# Define the pipeline
pipeline = Pipeline(stages=[
    document_assembler,
    sentence_detector,
    tokenizer,
    word_embeddings,
    clinical_ner,
    ner_converter,
    de_identification
])

# 3. Fit and Transform Data
# Since all components are pretrained (or DocumentAssembler, SentenceDetector etc don't require training in this context for transform)
# we can directly use transform for a pipeline with pretrained components.
# If you had a trainable component that wasn't already a PretrainedModel, you'd .fit() first.
# model = pipeline.fit(bq_df) # Not strictly necessary if all components are pretrained or don't require fitting on this specific data.
# deid_df = model.transform(bq_df)

# For pipelines composed of pretrained models and annotators like DocumentAssembler,
# you can often directly use a LightPipeline for smaller datasets or transform for larger ones.
# Given you're working with a "large dataframe", .transform() is appropriate.
# A pipeline model is created implicitly when you call fit, or you can create one with pretrained stages.

# It's good practice to create a PipelineModel
empty_df = spark.createDataFrame([[""]]).toDF("text_column_name") # Create an empty DataFrame to fit the pipeline (some annotators might expect this)
pipeline_model = pipeline.fit(empty_df) # Fitting on an empty DF for pretrained components essentially finalizes the pipeline structure.

deid_df = pipeline_model.transform(bq_df)

# 4. Select Output
# The 'deidentified_text' column will contain an array of AnnotatorType("document")
# Usually, the actual masked string is in the 'result' field of this annotation.
# You might need to explode or select the appropriate field.

# The DeIdentification annotator by default outputs a column with the deidentified text.
# The output column "deidentified_text" will contain the modified text directly at the row level.
# Let's check the schema to be sure
print("Schema of the de-identified DataFrame:")
deid_df.printSchema()

print("De-identified results:")
deid_df.select("text_column_name", "deidentified_text.result").show(truncate=False)

# If deidentified_text is an array (it usually is, representing the document),
# and you want the full text, you might need to access its elements.
# Often, the DeIdentification annotator when set to mask will directly produce a column
# where each row contains the full deidentified text.

# If the output in 'deidentified_text' is an array of annotation objects,
# you might need to extract the actual text. For DeIdentification, the primary output
# in the specified output column ('deidentified_text' here) is typically the masked text itself at the document level.

# Let's refine the selection based on typical DeIdentification output:
# The 'deidentified_text' column from the DeIdentification annotator usually stores
# the processed text where entities are masked.
# It outputs an array of annotations, but when it's the final stage for text processing,
# often the result is what you need.
# If 'deidentified_text' is an array of structures, you might need to explode or extract.
# However, often it's simpler:
result_df = deid_df.select("text_column_name", "deidentified_text.result") \
                   .withColumnRenamed("result", "masked_text_array")

# The 'result' from DeIdentification is usually an array with one element (the full processed document).
# So, we take the first element.
from pyspark.sql.functions import col
final_df = result_df.select("text_column_name", col("masked_text_array")[0].alias("masked_clinical_note"))

print("Final de-identified notes with fixed character masking:")
final_df.show(truncate=False)

# Stop the SparkSession
# spark.stop()




from pyspark.sql import SparkSession
import sparknlp
import sparknlp_jsl

# Assume SparkSession is obtained (Dataproc Serverless provides one)
spark = SparkSession.builder.appName("JSL_Dataproc_GCS_Secret").getOrCreate()

# --- Configuration for GCS Secret File ---
gcs_secret_file_path = "gs://your-secure-bucket/secrets/jsl_secret.txt" # CHANGE THIS
# ---------------------------------------

jsl_secret_value = None
try:
    print(f"Attempting to read JSL secret from GCS path: {gcs_secret_file_path}")
    # Read the file content. Spark can read GCS paths directly.
    # RDD an option for small files, or use Hadoop fs API for more control
    # For a small secret file, reading it as an RDD and collecting is straightforward.
    secret_rdd = spark.sparkContext.textFile(gcs_secret_file_path)
    jsl_secret_value = secret_rdd.first() # Assumes secret is on the first line

    if not jsl_secret_value or jsl_secret_value.strip() == "":
        raise ValueError("Secret file was empty or not read correctly.")

    jsl_secret_value = jsl_secret_value.strip() # Remove any leading/trailing whitespace
    print("JSL secret fetched successfully from GCS.")

    # Initialize Spark NLP JSL with the fetched secret
    spark = sparknlp_jsl.start(secret=jsl_secret_value)
    print("Spark NLP JSL started successfully using secret from GCS file.")

except Exception as e:
    # Be careful not to log the actual secret value in error messages here!
    print(f"Error reading secret from GCS or starting Spark NLP JSL: {e}")
    # If jsl_secret_value was populated, DO NOT log it.
    spark.stop()
    raise

# ... rest of your Spark NLP JSL code ...

# Example usage:
# data = [("Patient John Doe visited Dr. Smith.",)]
# df = spark.createDataFrame(data).toDF("text")
# ... process df using JSL pipeline ...

# spark.stop()
