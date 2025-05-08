from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor 
from pyspark.sql.types import *
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql.functions import col, avg, expr, explode, struct, posexplode, log2
from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.regression import RandomForestRegressor 
from pyspark.sql.functions import udf, collect_list
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, avg, expr, explode, struct, posexplode, lit, size, when, array_contains, sum as spark_sum, count as spark_count, log2, pow, rank 
from pyspark.sql.window import Window 
from pyspark.ml.evaluation import RankingEvaluator, RegressionEvaluator, MulticlassClassificationEvaluator 
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql.functions import udf, col
from pyspark.ml.linalg import SparseVector, VectorUDT
from pyspark.sql import Row
import numpy as np

# For HDFS
def run(command):
    print(f"Running command: {command}")
    result = os.popen(command).read()
    print(result)
    return result

# UDF struct -> SparseVector
@udf(returnType=VectorUDT())
def struct_to_sparse_vector_udf(struct_col):
    # Converts a struct column (assuming sparse vector format) to SparseVector.

    if struct_col is None or not isinstance(struct_col, Row):
        return None

    if 'size' not in struct_col or 'indices' not in struct_col or 'values' not in struct_col:
        return None

    size = struct_col['size']
    indices = struct_col['indices']
    values = struct_col['values']

    if size is None or (size > 0 and (indices is None or values is None)):
         return None

    if size > 0 and (not isinstance(indices, (list, tuple)) or not isinstance(values, (list, tuple)) or len(indices) != len(values)):
         return None

    if indices is not None:
         try:
            indices = [int(i) for i in indices]
         except (ValueError, TypeError):
             return None

    try:
        # Create SparseVector: SparseVector(size, indices, values)
        sparse_vector = SparseVector(size, indices, values)
        # print(f"Debug: Successfully created SparseVector: {sparse_vector}")
        return sparse_vector
    except Exception as e:
        # Catch potential errors during SparseVector creation (e.g., invalid indices)
        # print(f"Error creating SparseVector from struct {struct_col}: {e}. Returning None.")
        return None

# SparkSession initiation
team = "team27"
warehouse = "project/hive/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark ML - FeatureBasedRecommendation".format(team))\
        .master("yarn")\
        .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

print("SparkSession created.")

hdfs_train_json_path = "project/data/train"
hdfs_test_json_path = "project/data/test"

print(f"Loading training data from {hdfs_train_json_path}...")
train_df = spark.read.json(hdfs_train_json_path)
print(f"Loaded total {train_df.count()} training instances.")

# Sampling
sample_fraction = 1.0
seed = 42
print(f"Sampling training data to {sample_fraction*100}%...")
train_df = train_df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)
print(f"Using {train_df.count()} training instances after sampling.")

print(f"Loading test data from {hdfs_test_json_path}...")
test_df = spark.read.json(hdfs_test_json_path)
print(f"Loaded total {test_df.count()} test instances.")

print(f"Sampling test data to {sample_fraction*100}%...")
test_df = test_df.sample(withReplacement=False, fraction=sample_fraction, seed=seed)
print(f"Using {test_df.count()} test instances after sampling.")

# Take label from score
# Cast to IntegerType
print("\nCreating target variable (label) from score...")
train_df_with_label = train_df.withColumn("label", col("score").cast(IntegerType()))
test_df_with_label = test_df.withColumn("label", col("score").cast(IntegerType()))

print("Schema after adding label column:")
train_df_with_label.printSchema()

print("Assembling feature vector (features)...")

# Take features for VectorAssembler
assembler_input_cols = [
    "published_year",
    "ratings_count",
    "review_count",
    "author_idx", 
    "category_idx"
]

# Take off missing values
missing_cols_features = [c for c in assembler_input_cols if c not in train_df_with_label.columns]
if missing_cols_features:
    print(f"Warning: Following feature columns are missing from DataFrame: {missing_cols_features}. They will be excluded from the assembler input.")
    assembler_input_cols = [c for c in assembler_input_cols if c not in missing_cols_features]

if not assembler_input_cols:
     raise ValueError("No valid feature columns remaining for VectorAssembler after filtering and checking presence. Cannot proceed.")

print("Feature columns used for assembly:", assembler_input_cols)

# Make VectorAssembler for final feature vector
assembler = VectorAssembler(inputCols=assembler_input_cols,
                            outputCol="features",
                            handleInvalid="skip")


print("Applying VectorAssembler for features...")
train_df_with_features = assembler.transform(train_df_with_label)
test_df_with_features = assembler.transform(test_df_with_label)
print("VectorAssembler applied.")
all_initial_cols = train_df_with_label.columns 
cols_to_keep_explicitly = ["features", "label"] 

cols_to_drop = [c for c in all_initial_cols if c not in cols_to_keep_explicitly]

print(f"Dropping unused columns from DataFrame: {cols_to_drop}")
train_df_with_features = train_df_with_features.drop(*cols_to_drop)
test_df_with_features = test_df_with_features.drop(*cols_to_drop)

print("Dropping rows with null features or label...")
train_df_prepared_classification = train_df_with_features.dropna(subset=["features", "label"])
test_df_prepared_classification = test_df_with_features.dropna(subset=["features", "label"])

print("Train data prepared schema for classification:")
train_df_prepared_classification.printSchema()
print("Test data prepared schema for classification:")
test_df_prepared_classification.printSchema()

# RandomForestClassifier
print("--- Model 1: RandomForestClassifier ---")

# Make instance for RandomForestClassifier
rf_classifier = RandomForestClassifier(featuresCol="features", labelCol="label")

paramGrid_rf = ParamGridBuilder() \
    .addGrid(rf_classifier.numTrees, [10, 17, 25]) \
    .addGrid(rf_classifier.maxDepth, [5, 7, 10]) \
    .build()

print(f"ParamGrid for RandomForestClassifier has {len(paramGrid_rf)} combinations.")


# LogisticRegression
print("--- Model 2: LogisticRegression ---")

# Make instance for LogisticRegression
lr_classifier = LogisticRegression(featuresCol="features", labelCol="label", family="multinomial")

paramGrid_lr = ParamGridBuilder() \
    .addGrid(lr_classifier.regParam, [0.05, 0.17, 0.25]) \
    .addGrid(lr_classifier.elasticNetParam, [0.1, 0.25, 0.5]) \
    .build()

print(f"ParamGrid for LogisticRegression has {len(paramGrid_lr)} combinations.")


# Evaluator for CrossValidator (MulticlassClassification)
multiclass_evaluator_cv = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1") # F1 weighted


# CrossValidator Setup
num_folds_cv = 3
seed_cv = 43

print(f"Setting up CrossValidator with {num_folds_cv} folds")

# CrossValidator for RandomForestClassifier
cv_rf = CrossValidator(estimator=rf_classifier,
                       estimatorParamMaps=paramGrid_rf,
                       evaluator=multiclass_evaluator_cv,
                       numFolds=num_folds_cv,
                       parallelism=4,
                       seed=seed_cv)

# CrossValidator for LogisticRegression
cv_lr = CrossValidator(estimator=lr_classifier,
                       estimatorParamMaps=paramGrid_lr,
                       evaluator=multiclass_evaluator_cv,
                       numFolds=num_folds_cv,
                       parallelism=4,
                       seed=seed_cv)

# CrossValidator running
# CrossValidator trains models on folds train_df_prepared_classification
print("--- Running CrossValidator for RandomForestClassifier ---")

if 'train_df_prepared_classification' in locals() and train_df_prepared_classification is not None and train_df_prepared_classification.count() > 0:
    cv_model_rf = cv_rf.fit(train_df_prepared_classification)
    print("CrossValidator completed for RandomForestClassifier.")

    # Take best model
    best_model_rf = cv_model_rf.bestModel
    best_params_rf = cv_model_rf.getEstimatorParamMaps()[cv_model_rf.avgMetrics.index(max(cv_model_rf.avgMetrics))]
    print(f"\nBest parameters found by CV for RandomForestClassifier: {best_params_rf}")
    print(f"Average F1 (weighted) for best RandomForestClassifier during CV: {max(cv_model_rf.avgMetrics):.4f}")

else:
    print("Error: train_df_prepared_classification is not available or empty. Cannot run CrossValidator for RandomForestClassifier.")
    cv_model_rf = None
    best_model_rf = None
    best_params_rf = None

    
print("\n--- Running CrossValidator for LogisticRegression ---")
if 'train_df_prepared_classification' in locals() and train_df_prepared_classification is not None and train_df_prepared_classification.count() > 0:
    cv_model_lr = cv_lr.fit(train_df_prepared_classification)
    print("CrossValidator completed for LogisticRegression.")

    # Take best model
    best_model_lr = cv_model_lr.bestModel
    best_params_lr = cv_model_lr.getEstimatorParamMaps()[cv_model_lr.avgMetrics.index(max(cv_model_lr.avgMetrics))]
    print(f"\nBest parameters found by CV for LogisticRegression: {best_params_lr}")
    print(f"Average F1 (weighted) for best LogisticRegression during CV: {max(cv_model_lr.avgMetrics):.4f}")

else:
    print("Error: train_df_prepared_classification is not available or empty. Cannot run CrossValidator for LogisticRegression.")
    cv_model_lr = None
    best_model_lr = None
    best_params_lr = None
    
    
best_model_rf.write().overwrite().save("RFC")
best_model_lr.write().overwrite().save("LR")


print("\n--- Evaluating Best Models on Test Set ---")

if 'test_df_prepared_classification' in locals() and test_df_prepared_classification is not None and test_df_prepared_classification.count() > 0:

    # MulticlassClassificationEvaluator for final evaluation (Accuracy, F1, Precision, Recall)
    multiclass_evaluator_final_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1") # F1 weighted
    multiclass_evaluator_final_accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy") # Accuracy
    multiclass_evaluator_final_precision = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision") # Weighted Precision
    multiclass_evaluator_final_recall = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall") # Weighted Recall

    # Regression Evaluators for Classification Models (for RMSE and R2)
    regression_evaluator_final_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    regression_evaluator_final_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    # RandomForestClassifier evaluation
    if 'best_model_rf' in locals() and best_model_rf is not None:
        print("\nEvaluating RandomForestClassifier on test data...")
        rf_predictions_test = best_model_rf.transform(test_df_prepared_classification)

        # Classification metrics
        f1_rf_test = multiclass_evaluator_final_f1.evaluate(rf_predictions_test)
        accuracy_rf_test = multiclass_evaluator_final_accuracy.evaluate(rf_predictions_test)
        precision_rf_test = multiclass_evaluator_final_precision.evaluate(rf_predictions_test)
        recall_rf_test = multiclass_evaluator_final_recall.evaluate(rf_predictions_test)

        # Regression metrics
        rf_predictions_for_reg_eval = rf_predictions_test.withColumn("prediction", col("prediction").cast(DoubleType()))
        rf_predictions_for_reg_eval = rf_predictions_for_reg_eval.withColumn("label", col("label").cast(DoubleType()))

        rmse_rf_test = regression_evaluator_final_rmse.evaluate(rf_predictions_for_reg_eval)
        r2_rf_test = regression_evaluator_final_r2.evaluate(rf_predictions_for_reg_eval)

        print(f"RandomForestClassifier Test Metrics:")
        print(f"  Accuracy = {accuracy_rf_test:.4f}")
        print(f"  F1 (weighted) = {f1_rf_test:.4f}")
        print(f"  Precision (weighted) = {precision_rf_test:.4f}")
        print(f"  Recall (weighted) = {recall_rf_test:.4f}")
        print(f"  RMSE (predicted score) = {rmse_rf_test:.4f}")
        print(f"  R2 (predicted score) = {r2_rf_test:.4f}") 


    else:
        print("RandomForestClassifier best model not available. Skipping test evaluation.")
        f1_rf_test, accuracy_rf_test, precision_rf_test, recall_rf_test = None, None, None, None
        rmse_rf_test, r2_rf_test = None, None


    # LogisticRegression evaluation
    if 'best_model_lr' in locals() and best_model_lr is not None:
        print("\nEvaluating LogisticRegression on test data...")
        lr_predictions_test = best_model_lr.transform(test_df_prepared_classification)

        # Classification metrics
        f1_lr_test = multiclass_evaluator_final_f1.evaluate(lr_predictions_test)
        accuracy_lr_test = multiclass_evaluator_final_accuracy.evaluate(lr_predictions_test)
        precision_lr_test = multiclass_evaluator_final_precision.evaluate(lr_predictions_test)
        recall_lr_test = multiclass_evaluator_final_recall.evaluate(lr_predictions_test)

        # Regression metrics
        lr_predictions_for_reg_eval = lr_predictions_test.withColumn("prediction", col("prediction").cast(DoubleType()))
        # Cast label for DoubleType
        lr_predictions_for_reg_eval = lr_predictions_for_reg_eval.withColumn("label", col("label").cast(DoubleType()))

        rmse_lr_test = regression_evaluator_final_rmse.evaluate(lr_predictions_for_reg_eval)
        r2_lr_test = regression_evaluator_final_r2.evaluate(lr_predictions_for_reg_eval)

        print(f"LogisticRegression Test Metrics:")
        print(f"  Accuracy = {accuracy_lr_test:.4f}")
        print(f"  F1 (weighted) = {f1_lr_test:.4f}")
        print(f"  Precision (weighted) = {precision_lr_test:.4f}")
        print(f"  Recall (weighted) = {recall_lr_test:.4f}")
        print(f"  RMSE (predicted score) = {rmse_lr_test:.4f}")
        print(f"  R2 (predicted score) = {r2_lr_test:.4f}")


    else:
        print("LogisticRegression best model not available. Skipping test evaluation.")
        f1_lr_test, accuracy_lr_test, precision_lr_test, recall_lr_test = None, None, None, None
        rmse_lr_test, r2_lr_test = None, None


    # Model comparison
    print("\n--- Model Comparison ---")
    # Craete DataFrame for report
    comparison_data_cls = [
        ("RandomForestClassifier",
         f"Accuracy: {accuracy_rf_test:.4f}" if accuracy_rf_test is not None else "Accuracy: N/A",
         f"F1 (weighted): {f1_rf_test:.4f}" if f1_rf_test is not None else "F1: N/A",
         f"RMSE: {rmse_rf_test:.4f}" if rmse_rf_test is not None else "RMSE: N/A",
         f"R2: {r2_rf_test:.4f}" if r2_rf_test is not None else "R2: N/A"),
        ("LogisticRegression",
         f"Accuracy: {accuracy_lr_test:.4f}" if accuracy_lr_test is not None else "Accuracy: N/A",
         f"F1 (weighted): {f1_lr_test:.4f}" if f1_lr_test is not None else "F1: N/A",
         f"RMSE: {rmse_lr_test:.4f}" if rmse_lr_test is not None else "RMSE: N/A",
         f"R2: {r2_lr_test:.4f}" if r2_lr_test is not None else "R2: N/A"),
    ]

    # Update columns names RMSE и R2
    comparison_df_cls = spark.createDataFrame(comparison_data_cls, ["model", "Accuracy", "F1 (weighted)", "RMSE", "R2"])

    # Output comparison table
    print("Classification Model Performance Comparison:")
    comparison_df_cls.show(truncate=False)

    # Save comparison results
    evaluation_table_name_cls = f"{team}_projectdb.classification_evaluation_results"
    print(f"\nSaving classification evaluation results to Hive table: {evaluation_table_name_cls}")
    try:
        comparison_df_cls.coalesce(1)\
            .write\
            .mode("overwrite")\
            .format("csv")\
            .option("sep", ",")\
            .option("header","true")\
            .saveAsTable(evaluation_table_name_cls)
        print("Classification evaluation results saved.")
    except Exception as e:
        print(f"Error saving classification evaluation results to Hive: {e}")

    # Save best models
    print("Saving best classification models...")
    if 'best_model_rf' in locals() and best_model_rf is not None:
        rf_model_save_path = "project/models/classification_randomforest"
        print(f"Saving RandomForestClassifier model to {rf_model_save_path}...")
        try:
            best_model_rf.write().overwrite().save(rf_model_save_path)
            print(f"RandomForestClassifier model saved to {rf_model_save_path}")
        except Exception as e:
            print(f"Error saving RandomForestClassifier model: {e}")
    else:
         print("RandomForestClassifier best model not available. Skipping save.")


    if 'best_model_lr' in locals() and best_model_lr is not None:
        lr_model_save_path = "project/models/classification_logisticregression"
        print(f"Saving LogisticRegression model to {lr_model_save_path}...")
        try:
            best_model_lr.write().overwrite().save(lr_model_save_path)
            print(f"LogisticRegression model saved to {lr_model_save_path}")
        except Exception as e:
            print(f"Error saving LogisticRegression model: {e}")
    else:
         print("LogisticRegression best model not available. Skipping save.")


    # Save prediction results on test sample

    # For RandomForestClassifier
    if 'rf_predictions_test' in locals() and rf_predictions_test is not None and rf_predictions_test.count() > 0:
        print("\nSaving prediction results for RandomForestClassifier test set...")
        rf_predictions_output_path = "project/output/classification_randomforest_predictions"
        try:
            rf_predictions_test.select(
                col("label").alias("actual_label"),
                col("prediction").cast(IntegerType()).alias("predicted_label")
            ).coalesce(1)\
            .write\
            .mode("overwrite")\
            .format("csv")\
            .option("sep", ",")\
            .option("header","true")\
            .save(rf_predictions_output_path)
            print(f"Predicted labels for RandomForestClassifier test set saved to {rf_predictions_output_path}")
        except Exception as e:
            print(f"Error saving RandomForestClassifier predictions: {e}")
    else:
         print("RandomForestClassifier test predictions not available or empty. Skipping save.")

    # Для LogisticRegression
    if 'lr_predictions_test' in locals() and lr_predictions_test is not None and lr_predictions_test.count() > 0:
        print("\nSaving prediction results for LogisticRegression test set...")
        lr_predictions_output_path = "project/output/classification_logisticregression_predictions" 
        try:
            lr_predictions_test.select(
                col("label").alias("actual_label"),
                col("prediction").cast(IntegerType()).alias("predicted_label")
            ).coalesce(1)\
            .write\
            .mode("overwrite")\
            .format("csv")\
            .option("sep", ",")\
            .option("header","true")\
            .save(lr_predictions_output_path)
            print(f"Predicted labels for LogisticRegression test set saved to {lr_predictions_output_path}")
        except Exception as e:
            print(f"Error saving LogisticRegression predictions: {e}")
    else:
         print("LogisticRegression test predictions not available or empty. Skipping save.")


    # DataFrames cache clearing
    if 'rf_predictions_test' in locals() and rf_predictions_test: rf_predictions_test.unpersist()
    if 'lr_predictions_test' in locals() and lr_predictions_test: lr_predictions_test.unpersist()

else:
    print("Test data prepared for classification is not available or empty. Skipping all test evaluations and subsequent steps.")

    
print("Saving classification evaluation results to CSV...")

# Prepare data for csv
rf_metrics_available = all(v is not None for v in [accuracy_rf_test, f1_rf_test, precision_rf_test, recall_rf_test, rmse_rf_test, r2_rf_test]) if 'accuracy_rf_test' in locals() else False
lr_metrics_available = all(v is not None for v in [accuracy_lr_test, f1_lr_test, precision_lr_test, recall_lr_test, rmse_lr_test, r2_lr_test]) if 'accuracy_lr_test' in locals() else False


csv_comparison_data = []

if rf_metrics_available:
    csv_comparison_data.append(
        ("RandomForestClassifier",
         accuracy_rf_test,
         f1_rf_test,
         precision_rf_test,
         recall_rf_test,
         rmse_rf_test,
         r2_rf_test)
    )

if lr_metrics_available:
    csv_comparison_data.append(
        ("LogisticRegression",
         accuracy_lr_test,
         f1_lr_test,
         precision_lr_test,
         recall_lr_test,
         rmse_lr_test,
         r2_lr_test)
    )

# Initiate csv schema
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

csv_schema = StructType([
    StructField("model_name", StringType(), True),
    StructField("Accuracy", DoubleType(), True),
    StructField("F1_weighted", DoubleType(), True),
    StructField("Precision_weighted", DoubleType(), True),
    StructField("Recall_weighted", DoubleType(), True),
    StructField("RMSE", DoubleType(), True),
    StructField("R2", DoubleType(), True),
])

# Create Dataframe
csv_comparison_df = None
try:
    if csv_comparison_data:
        csv_comparison_df = spark.createDataFrame(csv_comparison_data, schema=csv_schema)
        print("DataFrame for CSV created.")
    else:
        print("No evaluation data available to create CSV DataFrame.")
except Exception as e:
    print(f"Error creating DataFrame for CSV: {e}")


# Output HDFS path for the CSV file
csv_output_path = "project/output/evaluation/"

if csv_comparison_df is not None:
    print(f"Saving comparison DataFrame to CSV at {csv_output_path}...")
    try:
        csv_comparison_df.coalesce(1)\
            .write\
            .mode("overwrite") \
            .option("header", "true") \
            .option("sep", ",") \
            .csv(csv_output_path)
        print("Classification evaluation results saved to CSV.")
    except Exception as e:
        print(f"Error saving classification evaluation results to CSV: {e}")
else:
    print("Skipping saving CSV as no evaluation data DataFrame was created.")
    
print("Saving Sample Data and Predictions to HDFS")

base_example_output_path = "project/output/example"

if 'prediction_sample' in locals() and prediction_sample is not None and prediction_sample.count() > 0:
    original_sample_path = f"{base_example_output_path}/original_sample"
    print(f"Saving original sample data to {original_sample_path}...")
    try:
        cols_to_save_original = ["label", "features"]
        if "book_title" in prediction_sample.columns:
            cols_to_save_original.append("book_title")

        prediction_sample.select(cols_to_save_original).coalesce(1)\
            .write\
            .mode("overwrite")\
            .parquet(original_sample_path)
        print("Original sample data saved.")
    except Exception as e:
        print(f"Error saving original sample data: {e}")
else:
    print("Original sample data not available or empty. Skipping save.")


if 'rf_sample_predictions' in locals() and rf_sample_predictions is not None and rf_sample_predictions.count() > 0:
    rf_predictions_sample_path = f"{base_example_output_path}/randomforest_predictions_sample"
    print(f"Saving RandomForestClassifier predictions on sample to {rf_predictions_sample_path}...")
    try:
        cols_to_save_rf = ["user_idx", "book_idx", "label", "features", "rawPrediction", "probability", "prediction"]
        if "book_title" in rf_sample_predictions.columns:
            cols_to_save_rf.append("book_title")

        rf_sample_predictions.select(cols_to_save_rf).coalesce(1)\
            .write\
            .mode("overwrite")\
            .parquet(rf_predictions_sample_path)
        print("RandomForestClassifier predictions on sample saved.")
    except Exception as e:
        print(f"Error saving RandomForestClassifier predictions on sample: {e}")
else:
    print("RandomForestClassifier prediction sample not available or empty. Skipping save.")


if 'lr_sample_predictions' in locals() and lr_sample_predictions is not None and lr_sample_predictions.count() > 0:
    lr_predictions_sample_path = f"{base_example_output_path}/logisticregression_predictions_sample"
    print(f"Saving LogisticRegression predictions on sample to {lr_predictions_sample_path}...")
    try:
        cols_to_save_lr = ["user_idx", "book_idx", "label", "features", "rawPrediction", "probability", "prediction"]
        if "book_title" in lr_sample_predictions.columns:
            cols_to_save_lr.append("book_title")

        lr_sample_predictions.select(cols_to_save_lr).coalesce(1)\
            .write\
            .mode("overwrite")\
            .parquet(lr_predictions_sample_path)
        print("LogisticRegression predictions on sample saved.")
    except Exception as e:
        print(f"Error saving LogisticRegression predictions on sample: {e}")
else:
    print("LogisticRegression prediction sample not available or empty. Skipping save.")

    
    



## Step 1: Prepare Feature Vector
def convert_struct_to_sparsevector(struct):
    if struct is None:
        return SparseVector(0, [], [])
    return SparseVector(struct.size, struct.indices, struct.values)


convert_udf = udf(convert_struct_to_sparsevector, VectorUDT())

tfidf_cols = ["description_tfidf", "summary_tfidf", "title_tfidf"]

for col_name in tfidf_cols:
    train_df = train_df.withColumn(col_name, convert_udf(col(col_name)))
    test_df = test_df.withColumn(col_name, convert_udf(col(col_name)))
 
    
numerical_cols = [
    "author_idx", "book_idx", "category_idx", "helpfulness_wilson",
    "published_day_encoded_cos", "published_day_encoded_sin",
    "published_month_encoded_cos", "published_month_encoded_sin",
    "published_year", "ratings_count", "review_count",
    "review_day_encoded_cos", "review_day_encoded_sin",
    "review_month_encoded_cos", "review_month_encoded_sin",
    "review_year", "user_idx"
]

train_df = train_df.dropna(subset =  numerical_cols)
test_df = test_df.dropna(subset =  numerical_cols)

assembler = VectorAssembler(
    inputCols=numerical_cols,
    outputCol="features"
)

gbt = GBTRegressor(
    labelCol="score",
    featuresCol="features",
    stepSize=0.2,
)

pipeline = Pipeline(stages=[assembler, gbt])

paramGrid = ParamGridBuilder()\
    .addGrid(gbt.maxDepth, [4, 6, 8])\
    .addGrid(gbt.featureSubsetStrategy, ["log2", "sqrt", "onethird"])\
    .addGrid(gbt.subsamplingRate, [0.6, 0.8, 1.0])\
    .build()

# Step 5: Define evaluator
evaluator = RegressionEvaluator(
    labelCol="score",
    predictionCol="prediction",
    metricName="rmse"
)

# Step 6: Cross-validation
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    collectSubModels=False
)

train_df.cache()
test_df.cache()

cv_model = cv.fit(train_df)


# Get the best model
best_model_gbt = cv_model.bestModel

predictions_gbt = best_model_gbt.transform(test_df)


rmse_gbt = evaluator.evaluate(predictions_gbt, {evaluator.metricName: "rmse"})
r2_gbt = evaluator.evaluate(predictions_gbt, {evaluator.metricName: "r2"})

model_path = "file:///home/team27/models/gbt"

best_model.write().overwrite().save(model_path)  



fm = FMRegressor(
    featuresCol="features",
    labelCol="score",
)

pipeline = Pipeline(stages=[assembler, fm])

paramGrid = ParamGridBuilder()\
    .addGrid(fm.factorSize, [4, 6, 8])\
    .addGrid(fm.initStd , [0.01, 0.1, 1.0])\
    .addGrid(fm.regParam, [0.01, 0.1, 1.0])\
    .build()

# Step 5: Define evaluator
evaluator = RegressionEvaluator(
    labelCol="score",
    predictionCol="prediction",
    metricName="rmse"
)

# Step 6: Cross-validation
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    collectSubModels=False
)

train_df.cache()
test_df.cache()


cv_model = cv.fit(train_df)
best_model_fm = cv_model.bestModel
predictions_fm = best_model_fm.transform(test_df)

rmse_fm = evaluator.evaluate(predictions_fm, {evaluator.metricName: "rmse"})
r2_fm = evaluator.evaluate(predictions_fm, {evaluator.metricName: "r2"})

model_path = "file:///home/team27/models/fm"

best_model.write().overwrite().save(model_path)  

# Create data frame to report performance of the models
models = [[str(best_model_gbt),rmse_gbt, r2_gbt], [str(best_model_fm),rmse_fm, r2_fm]]

#temp = list(map(list, models.items()))
df = spark.createDataFrame(models, ["model", "RMSE", "R2"])
df.show(truncate=False)

# Save it to HDFS
df.coalesce(1)\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .save("project/output/evaluation2")
