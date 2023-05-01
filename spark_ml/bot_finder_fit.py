import operator
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier

MODEL_PATH = 'spark_ml_model'
LABEL_COL = 'is_bot'


def process(spark, data_path, model_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param data_path: путь до датасета
    :param model_path: путь сохранения обученной модели
    """
    df = spark.read.parquet(data_path)

    user_type_index = StringIndexer(inputCol='user_type', outputCol="user_type_index")

    platform_index = StringIndexer(inputCol='platform', outputCol="platform_index")
    feature = VectorAssembler(
        inputCols=['duration', 'item_info_events', 'select_item_events', 'make_order_events', 'events_per_min',
                   'user_type_index', 'platform_index'],
        outputCol="features")

    rf_classifier = RandomForestClassifier(labelCol="is_bot", featuresCol="features")

    pipeline = Pipeline(stages=[user_type_index, platform_index, feature, rf_classifier])

    p_model = pipeline.fit(df)

    evaluator = MulticlassClassificationEvaluator(labelCol="is_bot", predictionCol="prediction", metricName="accuracy")

    paramGrid = ParamGridBuilder() \
        .addGrid(rf_classifier.maxDepth, [2, 3, 4]) \
        .addGrid(rf_classifier.maxBins, [4, 5, 6]) \
        .addGrid(rf_classifier.minInfoGain, [0.05, 0.1, 0.15]) \
        .build()

    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=paramGrid,
                               evaluator=evaluator,
                               trainRatio=0.8)

    model = tvs.fit(df)

    model.bestModel.write().overwrite().save(model_path)


def main(data_path, model_path):
    spark = _spark_session()
    process(spark, data_path, model_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='session-stat.parquet', help='Please set datasets path.')
    parser.add_argument('--model_path', type=str, default=MODEL_PATH, help='Please set model path.')
    args = parser.parse_args()
    data_path = args.data_path
    model_path = args.model_path
    main(data_path, model_path)
