from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
from pyspark.ml.feature import HashingTF, IDF, IDFModel

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import NaiveBayes

from pyspark.ml import Pipeline, PipelineModel

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def mapper(s):
	label = int(s.split("||")[0])
	sentence = s.split("||")[1]
	return (label, sentence)

sc = SparkContext("local[2]", "pretrain")

spark = SparkSession\
		.builder\
		.appName("pretrain")\
		.getOrCreate()

trainData = []
trainLabels = []



documents = sc.textFile("3_1-6_30.txt")
test = sc.textFile("5_3-5_5.txt")

schema = StructType([
StructField("label", IntegerType(), True),
StructField("sentence", StringType(), True)])

# create dataframe for training data
df = spark.createDataFrame(documents.map(mapper), schema)
testDf = spark.createDataFrame(test.map(mapper), schema)
# (trainingData, testData) = df.randomSplit([0.9, 0.1])
# tokenize the sentence
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenized = tokenizer.transform(df)

# remove stop words
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
removed = remover.transform(tokenized)

# tf-idf
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=300)
featurizedData = hashingTF.transform(removed)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
# idfModel.save("idf_model.model")
# idfLoad = IDFModel.load("idf_model.model")

# labelize the words and TF
# indexer = StringIndexer(inputCol="filtered", outputCol="filteredIndex")
# indexed = indexer.fit(removed).transform(removed)
# vector = CountVectorizer(inputCol="filtered", outputCol="features")
# vectored = vector.fit(removed).transform(removed)
# vectorFitted = vector.fit(removed)
# vectorFitted.save("vector_model.model")
# vectorLoad = CountVectorizerModel.load("vector_model.model")


# train the model using Logistic Regression
# lr = LogisticRegression(maxIter=5, regParam=0.3, elasticNetParam=0.8, featuresCol="features", labelCol="label", family="multinomial")
nb = NaiveBayes(smoothing=1.2, modelType="multinomial")
# lrModel = lr.fit(vectored)

# build pipeline
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idfModel, nb])
# pipeline = Pipeline(stages=[tokenizer, remover, vectorLoad, nb])
# print("Coefficients: " + str(lrModel.coefficientMatrix))
# print("Intercept: " + str(lrModel.interceptVector))

# train model by pipeline
model = pipeline.fit(df)
model.save("pipeline_model.model")
modelLoad = PipelineModel.load("pipeline_model.model")
# make prediction
predictions = modelLoad.transform(testDf)
predictions.select("prediction", "label").show(40)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

