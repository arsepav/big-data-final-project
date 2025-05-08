import math
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.param import Params
from pyspark.ml.feature import StringIndexer, CountVectorizer, Tokenizer, StopWordsRemover, HashingTF, IDF, Word2Vec
from pyspark.ml import Pipeline, Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark import keyword_only
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark import StorageLevel

class MonthAwareCyclicalEncoder(Transformer, HasInputCol, HasOutputCol,
                              DefaultParamsReadable, DefaultParamsWritable):

    period_type = Param(Params._dummy(),
                        "period_type", "Type of period (month/day)")
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None, period_type="month"):
        super(MonthAwareCyclicalEncoder, self).__init__()
        self._setDefault(period_type=period_type)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
    @keyword_only
    def setParams(self, inputCol=None, outputCol=None, period_type="month"):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        period_type = self.getOrDefault("period_type")

        if period_type == "month":
            # For months, always use 12 as period
            return (dataset
                   .withColumn(f"{output_col}_sin", F.sin(2 * math.pi * F.col(input_col) / 12))
                   .withColumn(f"{output_col}_cos", F.cos(2 * math.pi * F.col(input_col) / 12)))
        elif period_type == "day":
            # For days, calculate based on month
            return (dataset
                   .withColumn("days_in_month",
                              F.dayofmonth(F.last_day(F.col("review_datetime"))))
                   .withColumn(f"{output_col}_sin",
                              F.sin(2 * math.pi * F.col(input_col) / F.col("days_in_month")))
                   .withColumn(f"{output_col}_cos",
                              F.cos(2 * math.pi * F.col(input_col) / F.col("days_in_month")))
                   .drop("days_in_month"))
        else:
            raise ValueError("period_type must be either 'month' or 'day'")


# Add here your team number teamx
team = 'team27'

# location of your Hive database in HDFS
warehouse = "project/warehouse"

spark = SparkSession.builder\
        .appName("{} - spark preprocessing".format(team))\
        .master("yarn")\
        .config("spark.hadoop.hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883")\
        .config("spark.sql.warehouse.dir", warehouse)\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .enableHiveSupport()\
        .getOrCreate()

books_df = spark.read.format("avro").table('team27_projectdb.books')

reviews_df = spark.read.format("avro").table('team27_projectdb.reviews_processed')


# Join on book_title
combined_df = reviews_df.join(books_df, reviews_df.book_title == books_df.title, "inner")

# Fill ratings_count with reviews_dataset values
# Step 1: Count number of reviews per book
book_review_counts = combined_df.groupBy("book_id").agg(F.count("*").alias("review_count"))

# Step 2: Join back to get imputation candidates
combined_df = combined_df.join(book_review_counts, on="book_id", how="left")

# Step 3: Fill ratings_count where NULL using review_count
combined_df = combined_df.withColumn(
    "ratings_count",
    F.when(F.col("ratings_count").isNull(), F.col("review_count"))
     .otherwise(F.col("ratings_count"))
)

# Cast to integer
combined_df = combined_df.withColumn("ratings_count", F.col("ratings_count").cast("int"))


# Filter out rows where user_id or book_id are null (essential for ALS)
combined_df = combined_df.filter(
    F.col("user_id").isNotNull() & F.col("book_id").isNotNull()
)

# Filter out rows where fields have nonimputable null values
combined_df = combined_df.filter(
    F.col("publisher").isNotNull() & F.col("authors").isNotNull() & F.col("categories").isNotNull()
)

# Cast score to float
combined_df = combined_df.withColumn("score", F.col("score").cast("float"))

# Fill missing text-based fields with empty string (safe for NLP use)
combined_df = combined_df.fillna({"summary": "", "review_text": "", "description": ""})

# Split helpfulness into helpful and total
combined_df = combined_df.withColumn("helpful_yes", F.split(
    F.col("helpfulness"), "/").getItem(0).cast("float"))
combined_df = combined_df.withColumn("helpful_total",
                                     F.split(F.col("helpfulness"), "/").getItem(1).cast("float"))

# Wilson lower bound helpfulness score
z = 1.96  # 95% confidence
combined_df = combined_df.withColumn(
    "helpfulness_wilson",
    F.expr(f"""
        CASE 
            WHEN helpful_total = 0 THEN 0.0
            ELSE (
                (helpful_yes + {z*z}/2) / (helpful_total + {z*z}) -
                {z} * SQRT((helpful_yes * (helpful_total - helpful_yes)) / helpful_total + {z*z}/4) / (helpful_total + {z*z})
            )
        END
    """)
)

combined_df = combined_df.withColumn(
    "helpfulness_wilson", F.col("helpfulness_wilson").cast("float"))

helpful_cols_to_drop = [
    "helpful_yes", "helpful_total", "helpfulness"
]

combined_df = combined_df.drop(*helpful_cols_to_drop)


# Handle datetime transformation for review_time
# Convert Unix timestamp to datetime and extract components
combined_df = combined_df.withColumn(
    "review_datetime", F.from_unixtime(F.col("review_time")/1000).cast("timestamp"))

combined_df = combined_df.withColumn("review_year", F.year("review_datetime"))
combined_df = combined_df.withColumn("review_month", F.month("review_datetime"))
combined_df = combined_df.withColumn("review_day", F.dayofmonth("review_datetime"))

# Apply cyclical encoding using CustomTransformer
month_encoder = MonthAwareCyclicalEncoder(
    inputCol="review_month",
    outputCol="review_month_encoded",
    period_type="month"
)

day_encoder = MonthAwareCyclicalEncoder(
    inputCol="review_day",
    outputCol="review_day_encoded",
    period_type="day"
)

# Create a pipeline for the temporal transformations
temporal_pipeline = Pipeline(stages=[month_encoder, day_encoder])
combined_df = temporal_pipeline.fit(combined_df).transform(combined_df)

# Handle published_date from books
combined_df = combined_df.withColumn("published_date",
                                     F.to_date(F.col("published_date"), "yyyy-MM-dd"))
combined_df = combined_df.withColumn("published_year", F.year("published_date"))
combined_df = combined_df.withColumn("published_month", F.month("published_date"))
combined_df = combined_df.withColumn("published_day", F.dayofmonth("published_date"))

# Apply cyclical encoding for published date using CustomTransformer
pub_month_encoder = MonthAwareCyclicalEncoder(
    inputCol="published_month",
    outputCol="published_month_encoded",
    period_type="month"
)

pub_day_encoder = MonthAwareCyclicalEncoder(
    inputCol="published_day",
    outputCol="published_day_encoded",
    period_type="day"
)
pub_temporal_pipeline = Pipeline(stages=[pub_month_encoder, pub_day_encoder])
combined_df = pub_temporal_pipeline.fit(combined_df).transform(combined_df)

# Drop original columns that we encoded
cyclical_cols_to_drop = [
    "review_time", "review_datetime", "review_month", "review_day", 
    "published_date", "published_month", "published_day"
]

combined_df = combined_df.drop(*cyclical_cols_to_drop)


def parse_authors(authors_str):
    authors_str = authors_str.strip()
    if authors_str.startswith("{") and authors_str.endswith("}"):
        inner = authors_str[1:-1]
        parts = [a.strip().strip('"') for a in inner.split(",")]
        return parts[0]
    return authors_str.strip().strip('"')


def split_category(category_string):
    # Match content within {} or {""}
    matches = re.findall(r'\{("?)(.*?)\1\}', category_string)
    categories = []
    for _, text in matches:
        # If the original text was quoted (indicating possible multiple categories)
        if ' ' in text and not text.islower() and not text.isupper():
            # Split on &
            parts = text.split('&')
            categories.extend(parts)
        else:
            categories.append(text)
    return categories[0]


# Handle categorical features (authors, categories, user_id, book_id)
# Authors processing
parse_authors_udf = F.udf(parse_authors, StringType())
combined_df = combined_df.withColumn(
    "author",
    parse_authors_udf("authors")
)

# Drop original column that we preprocessed
combined_df = combined_df.drop("authors")

# Categories processing
split_categories_udf = F.udf(split_category, StringType())

# Apply to the categories column
combined_df = combined_df.withColumn(
    "category",
    split_categories_udf(F.col("categories"))
)

# Drop original column that we preprocessed
combined_df = combined_df.drop("categories")


# String fields that need indexing
author_indexer = StringIndexer(inputCol="author", outputCol="author_idx")
category_indexer = StringIndexer(inputCol="category", outputCol="category_idx")
user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx")
book_indexer = StringIndexer(inputCol="book_id", outputCol="book_idx")


# Text fields to process
text_fields = ["description", "review_text", "summary", "title", "publisher"]

# Shared stages: tokenization + stopwords removal
tokenizers = [Tokenizer(inputCol=col, outputCol=f"{col}_tokens") for col in text_fields]
removers = [StopWordsRemover(inputCol=f"{col}_tokens", 
                             outputCol=f"{col}_filtered") for col in text_fields]

# TF-IDF stages
hashers = [HashingTF(inputCol=f"{col}_filtered",
                     outputCol=f"{col}_tf", numFeatures=300) for col in text_fields]
idfs = [IDF(inputCol=f"{col}_tf", outputCol=f"{col}_tfidf") for col in text_fields]

# Word2Vec stages
word2vecs = [Word2Vec(inputCol=f"{col}_filtered",
                      outputCol=f"{col}_w2v", vectorSize=100, minCount=2) for col in text_fields]


# Build TF-IDF pipeline
tfidf_pipeline = Pipeline(stages=tokenizers + removers + hashers + idfs + [
    author_indexer, category_indexer, user_indexer, book_indexer
])

# Fit and transform
tfidf_model = tfidf_pipeline.fit(combined_df)
tfidf_df = tfidf_model.transform(combined_df)

# Remove original columns that we encoded
cols_to_drop = [
    "book_id", "user_id", "author", 
    "description", "review_text", "summary", 
    "title", "publisher", "category"
]
tfidf_df = tfidf_df.drop(*cols_to_drop)

intermediate_cols = [f"{col}_{suffix}" for col
                     in text_fields for suffix in ["tokens", "filtered", "tf"]]
tfidf_df = tfidf_df.drop(*intermediate_cols)


(train_data, test_data) = tfidf_df.randomSplit([0.7, 0.3], seed=42)

train_data.coalesce(5)\
    .write\
    .mode("overwrite")\
    .format("json")\
    .save("project/data/train")

test_data.coalesce(2)\
    .write\
    .mode("overwrite")\
    .json("project/data/test")