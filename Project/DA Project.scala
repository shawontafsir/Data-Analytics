// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC ## Overview
// MAGIC
// MAGIC This notebook is generated for Data Analytics project. The project is to predict fare of yellow taxi in NewYork. Various regression model are used to build a model that is trained on the dataset received from Kaggle. The dataset is specically for January 2020 named yellow_tripdata_2020_01.csv. It is approximately 600 MB in size.
// MAGIC
// MAGIC Due to the big data size, this project has been conducted using Spark. Scala is mainly used as a programming language for data access and manipulating. Python is used particularly for plot graphs.



import org.apache.spark.sql.functions.col

// # File location and type
val file_location = "/FileStore/tables/yellow_tripdata_2020_01.csv"
val file_type = "csv"

// # CSV options
val infer_schema = "false"
val first_row_is_header = "true"
val delimiter = ","

// # The applied options are for CSV files. For other file types, these will be ignored.
val df = spark.read.format(file_type)
  .option("inferSchema", infer_schema)
  .option("header", first_row_is_header)
  .option("sep", delimiter)
  .load(file_location)

df.cache()

var selectedData = df.select(
  col("trip_distance").cast("double"),
  col("PULocationID").cast("double"), 
  col("DOLocationID").cast("double"), 
  col("RateCodeID").cast("double"), 
  col("fare_amount").cast("double")
)

display(selectedData.describe())
println(selectedData.count())



// Count noisy data
println(selectedData.where(col("trip_distance")<=0 || col("fare_amount")<=0 || col("RateCodeID").isNull).count)

// Removing noisy data observed from the statistics
selectedData = selectedData.selectExpr("*", "fare_amount / trip_distance as fareByDistance")
                          .where(col("trip_distance")>0 && col("fare_amount")>0 && col("RateCodeID").isNotNull)

// display(selectedData.orderBy('trip_distance.asc))
println(selectedData.count)

// Create a view or table
selectedData.createOrReplaceTempView("selected_data")



// MAGIC %python
// MAGIC
// MAGIC pyData = spark.sql(
// MAGIC   """
// MAGIC   select * from selected_data
// MAGIC   """
// MAGIC )



// MAGIC %python
// MAGIC
// MAGIC # Use Python to create plots
// MAGIC import matplotlib.pyplot as plt
// MAGIC
// MAGIC # Data
// MAGIC y_values = pyData.select("fareByDistance").rdd.flatMap(lambda x: x).collect()
// MAGIC
// MAGIC # Create a box plot with all values
// MAGIC plt.subplot(1, 2, 1)
// MAGIC plt.subplots_adjust(wspace=0.8)
// MAGIC plt.boxplot(y_values)
// MAGIC plt.title("Box Plot with Ouliers")
// MAGIC plt.ylabel("Fare By Distance")
// MAGIC
// MAGIC # Create a box plot without outliers
// MAGIC plt.subplot(1, 2, 2)
// MAGIC plt.boxplot(y_values, 0, '')
// MAGIC plt.title("Box Plot without outliers")
// MAGIC plt.ylabel("Fare By Distance")
// MAGIC
// MAGIC # Show the plot
// MAGIC plt.show()



import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

def findOutliers(df: DataFrame, columns: Array[String]): DataFrame = {
  // Identifying the numerical columns in a Spark DataFrame
  // val numericColumns = df.dtypes.filter(_._2 == "Integer").map(_._1)
  val numericColumns = columns

  // Define the UDF to check if a value is an outlier
  val isOutlier = udf((value: Int, Q1: Double, Q3: Double) => {
    val IQR = Q3 - Q1
    val lowerThreshold = Q1 - 1.5 * IQR
    val upperThreshold = Q3 + 1.5 * IQR
    if (value < lowerThreshold || value > upperThreshold) 1 else 0
  })

  var updatedDF = df

  // Using the `for` loop to create new columns by identifying the outliers for each feature
  for (column <- numericColumns) {
    val Q1 = updatedDF.stat.approxQuantile(column, Array(0.25), 0)(0)
    val Q3 = updatedDF.stat.approxQuantile(column, Array(0.75), 0)(0)

    val isOutlierCol = s"is_outlier_$column"

    updatedDF = updatedDF
      .withColumn(isOutlierCol, isOutlier(col(column), lit(Q1), lit(Q3)))
  }

  // Selecting the specific columns which we have added above
  val selectedColumns = updatedDF.columns.filter(_.startsWith("is_outlier"))

  // Adding all the outlier columns into a new column "total_outliers"
  updatedDF = updatedDF.withColumn("total_outliers", selectedColumns.map(col).reduce(_ + _))

  // Dropping the extra columns created above
  updatedDF = updatedDF.drop(selectedColumns: _*)

  updatedDF
}

// Usage:
// val outliersDF = findOutliers(yourDataFrame)
// outliersDF.show()



// remove outliers
val outliersDF: DataFrame = findOutliers(selectedData, columns=Array("fareByDistance"))
println(outliersDF.where(col("total_outliers") > 0).count)

selectedData = outliersDF.where(col("total_outliers") === 0)
println(selectedData.count())



import org.apache.spark.ml.regression._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}

// Step 4: Split the DataFrame into a training set and a test set (80% train, 20% test)
val Array(trainData, testData) = selectedData.randomSplit(Array(0.8, 0.2))

// val rFormula = new RFormula()

// Step 5: Prepare features and labels using a VectorAssembler
val assembler = new VectorAssembler()
  .setInputCols(Array("trip_distance", "PULocationID", "DOLocationID", "RateCodeID"))
  .setOutputCol("features")
  .setHandleInvalid("skip")

val sScalar = new StandardScaler().setInputCol("features")

// val regressionClasses = List("LinearRegression", "DecisionTreeRegressor", "RandomForestRegressor", "GBTRegressor")

// Step 6: Create a Linear Regression model
val lR = new LinearRegression()
  .setLabelCol("fare_amount")
  .setFeaturesCol("features")
  .setPredictionCol("prediction")

val dTR = new DecisionTreeRegressor()
  .setLabelCol("fare_amount")
  .setFeaturesCol("features")
  .setPredictionCol("prediction")

val rFR = new RandomForestRegressor()
  .setLabelCol("fare_amount")
  .setFeaturesCol("features")
  .setPredictionCol("prediction")


println(lR.explainParams())
println(dTR.explainParams())
println(rFR.explainParams())


val pipelineLR = new Pipeline().setStages(Array(assembler, sScalar, lR))
val pipelineDTR = new Pipeline().setStages(Array(assembler, sScalar, dTR))
val pipelineRFR = new Pipeline().setStages(Array(assembler, sScalar, rFR))



// specifying different combinations of hyperparameters to select the best model using an Evaluator, testing their predictions
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.RegressionEvaluator

// Params for linear regressor
val paramsLR = new ParamGridBuilder()
  .addGrid(lR.elasticNetParam, Array(0.0, 0.5, 1.0))
  .addGrid(lR.regParam, Array(0.1, 0.5, 0.9))
  .addGrid(lR.maxIter, Array(50, 100, 200))
  .build()

// Params for Decision Tree regressors
val paramsDTR = new ParamGridBuilder()
  .addGrid(dTR.maxDepth, Array(3, 5, 10))
  .addGrid(dTR.minInfoGain, Array(0, 0.5))
  .addGrid(dTR.minInstancesPerNode, Array(1, 2, 5, 10))
  .build()

// Params for Random forests regressors
val paramsRFR = new ParamGridBuilder()
  .addGrid(rFR.numTrees, Array(10, 20, 30, 40))
  .addGrid(rFR.maxDepth, Array(5, 10))
  .build()



// Evaluator using RegressionEvaluator using rmse
val rmse_evaluator = new RegressionEvaluator()
  .setLabelCol("fare_amount")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

// Evaluator using RegressionEvaluator using mae
val mae_evaluator = new RegressionEvaluator()
  .setLabelCol("fare_amount")
  .setPredictionCol("prediction")
  .setMetricName("mae")

// Evaluator using RegressionEvaluator using r2
val r2_evaluator = new RegressionEvaluator()
  .setLabelCol("fare_amount")
  .setPredictionCol("prediction")
  .setMetricName("r2")



// Define Train Validation Split
import org.apache.spark.ml.tuning.TrainValidationSplit

// Linear regressor
val tVS_LR = new TrainValidationSplit()
  .setTrainRatio(0.75) // also the default.
  .setEstimatorParamMaps(paramsLR).setEstimator(pipelineLR)
  .setEvaluator(rmse_evaluator)

// Decision Tree regressor
val tVS_DTR = new TrainValidationSplit()
  .setTrainRatio(0.75) // also the default.
  .setEstimatorParamMaps(paramsDTR).setEstimator(pipelineDTR)
  .setEvaluator(rmse_evaluator)

// Random Forests regressor
val tVS_RFR = new TrainValidationSplit()
  .setTrainRatio(0.75) // also the default.
  .setEstimatorParamMaps(paramsRFR).setEstimator(pipelineRFR)
  .setEvaluator(rmse_evaluator)


// Get TrainValidationSplitModel for linear regressor
val tVS_LR_Model = tVS_LR.fit(trainData)



// Get TrainValidationSplitModel for decision tree regressor
val tVS_DTR_Model = tVS_DTR.fit(trainData)



// Get TrainValidationSplitModel for random forests regressor
val tVS_RFR_Model = tVS_RFR.fit(trainData)



// Get best model and statistics
import org.apache.spark.ml.regression.{LinearRegressionModel, DecisionTreeRegressionModel, RandomForestRegressionModel, GBTRegressionModel}

val bestPipelineLRModel = tVS_LR_Model.bestModel.asInstanceOf[PipelineModel]
val bestLRModel = bestPipelineLRModel.stages.last.asInstanceOf[LinearRegressionModel]

val summary = bestLRModel.summary
println(summary)
println(s"numIterations: ${summary.totalIterations}")
println(s"Co-efficients: ${bestLRModel.coefficients} Intercept: ${bestLRModel.intercept}")
summary.residuals.show()
println(summary.objectiveHistory.toSeq.toDF.show())
println(summary.objectiveHistory)
println(summary.rootMeanSquaredError)
println(summary.r2)

val bestPipelineDTRModel = tVS_DTR_Model.bestModel.asInstanceOf[PipelineModel]
val bestDTRModel = bestPipelineDTRModel.stages.last.asInstanceOf[DecisionTreeRegressionModel]

val summaryLR = bestLRModel.summary
println(summaryLR)

val bestPipelineRFRModel = tVS_RFR_Model.bestModel.asInstanceOf[PipelineModel]
val bestRFRModel = bestPipelineRFRModel.stages.last.asInstanceOf[RandomForestRegressionModel]



// Step 7: Make predictions on the test data
// val testPreprocessed = assembler.transform(testData)
val predictionsLR = bestPipelineLRModel.transform(testData)
predictionsLR.show(false)

val predictionsDTR = bestPipelineDTRModel.transform(testData)
predictionsDTR.show()

val predictionsRFR = bestPipelineRFRModel.transform(testData)
predictionsRFR.show()



// RMSE
val rmseLR = rmse_evaluator.evaluate(predictionsLR)
println(s"Root Mean Squared Error (RMSE) for LR: $rmseLR")

val rmseDTR = rmse_evaluator.evaluate(predictionsDTR)
println(s"Root Mean Squared Error (RMSE) for DTR: $rmseDTR")

val rmseRFR = rmse_evaluator.evaluate(predictionsRFR)
println(s"Root Mean Squared Error (RMSE) for RFR: $rmseRFR")


// MAE
val maeLR = mae_evaluator.evaluate(predictionsLR)
println(s"Mean Absolute Error (MAE) for LR: $maeLR")

val maeDTR = mae_evaluator.evaluate(predictionsDTR)
println(s"Mean Absolute Error (MAE) for DTR: $maeDTR")

val maeRFR = mae_evaluator.evaluate(predictionsRFR)
println(s"Mean Absolute Error (MAE) for RFR: $maeRFR")


// R2
val r2LR = r2_evaluator.evaluate(predictionsLR)
println(s"R-squared (r2) Error for LR: $r2LR")

val r2DTR = r2_evaluator.evaluate(predictionsDTR)
println(s"R-squared (r2) Error for DTR: $r2DTR")

val r2RFR = r2_evaluator.evaluate(predictionsRFR)
println(s"R-squared (r2) Error for RFR: $r2RFR")



import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

val evaluatedDataMap = Seq(
  Map("Regression Model" -> "Linear", "RMSE" -> rmseLR, "MAE" -> maeLR, "R2" -> r2LR),
  Map("Regression Model" -> "Decision Tree", "RMSE" -> rmseDTR, "MAE" -> maeDTR, "R2" -> r2DTR),
  Map("Regression Model" -> "Random Forest Tree", "RMSE" -> rmseRFR, "MAE" -> maeRFR, "R2" -> r2RFR)
)

// Define the schema based on the keys and types of the first map
val schema = new StructType()
    .add("Regression Model",StringType)
    .add("RMSE", DoubleType)
    .add("MAE", DoubleType)
    .add("R2", DoubleType)

// Convert the sequence of maps to a sequence of Rows
val rows = evaluatedDataMap.map { rowMap =>
  Row.fromSeq(schema.map(field => rowMap.getOrElse(field.name, null)))
}

// Create a DataFrame
val evaluationMatrix = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

display(evaluationMatrix.select(col("Regression Model"), round('RMSE, 6).as("RMSE"), round('MAE, 6).alias("MAE"), round('R2, 6).alias("R2")))



// create sql table view to access data while using python
predictionsLR.createOrReplaceTempView("predictions_lr")
predictionsDTR.createOrReplaceTempView("predictions_dtr")
predictionsRFR.createOrReplaceTempView("predictions_rfr")



// MAGIC %python
// MAGIC
// MAGIC import numpy as np
// MAGIC
// MAGIC # Create python dataframe
// MAGIC
// MAGIC predictionsLR = spark.sql(
// MAGIC     """
// MAGIC     select * from predictions_lr
// MAGIC     """
// MAGIC )
// MAGIC
// MAGIC predictionsDTR = spark.sql(
// MAGIC     """
// MAGIC     select * from predictions_dtr
// MAGIC     """
// MAGIC )
// MAGIC
// MAGIC predictionsRFR = spark.sql(
// MAGIC     """
// MAGIC     select * from predictions_rfr
// MAGIC     """
// MAGIC )
// MAGIC
// MAGIC
// MAGIC # Data Preparation of actual and predicted fares
// MAGIC models = {
// MAGIC     "lr": "Linear Regression",
// MAGIC     "dtr": "Decision Tree Regression",
// MAGIC     "rfr": "Random Forest Tree Regression"
// MAGIC }
// MAGIC
// MAGIC actual_fares = {
// MAGIC     "lr": np.array(predictionsLR.select("fare_amount").rdd.flatMap(lambda x: x).collect()),
// MAGIC     "dtr": np.array(predictionsDTR.select("fare_amount").rdd.flatMap(lambda x: x).collect()),
// MAGIC     "rfr": np.array(predictionsRFR.select("fare_amount").rdd.flatMap(lambda x: x).collect()),
// MAGIC }
// MAGIC predicted_fares = {
// MAGIC     "lr": np.array(predictionsLR.select("prediction").rdd.flatMap(lambda x: x).collect()),
// MAGIC     "dtr": np.array(predictionsDTR.select("prediction").rdd.flatMap(lambda x: x).collect()),
// MAGIC     "rfr": np.array(predictionsRFR.select("prediction").rdd.flatMap(lambda x: x).collect())
// MAGIC }
// MAGIC residuals = {
// MAGIC     "lr": actual_fares["lr"] - predicted_fares["lr"],
// MAGIC     "dtr": actual_fares["dtr"] - predicted_fares["dtr"],
// MAGIC     "rfr": actual_fares["rfr"] - predicted_fares["rfr"]
// MAGIC }



// MAGIC %python
// MAGIC # Use Python to create plots
// MAGIC import matplotlib.pyplot as plt
// MAGIC
// MAGIC # Create scatter plots
// MAGIC # Increase total size by setting figsize
// MAGIC plt.figure(figsize=(8, 4))
// MAGIC for i, k in enumerate(actual_fares.keys()):
// MAGIC     plt.subplot(1, 3, i+1)
// MAGIC     plt.subplots_adjust(wspace=1)
// MAGIC     plt.scatter(actual_fares[k], predicted_fares[k])
// MAGIC     plt.xlabel("Actual fare")
// MAGIC     plt.ylabel("Predicted fare")
// MAGIC     plt.title(models[k])
// MAGIC plt.show()
// MAGIC
// MAGIC # Create histogram of residuals
// MAGIC # Increase total size by setting figsize
// MAGIC plt.figure(figsize=(8, 4))
// MAGIC for i, k in enumerate(residuals.keys()):
// MAGIC     plt.subplot(1, 3, i+1)
// MAGIC     plt.subplots_adjust(wspace=1)
// MAGIC     plt.hist(residuals[k], bins=60, edgecolor='black', alpha=0.8)
// MAGIC     # Set x-axis range from -20 to 20
// MAGIC     plt.xlim(-15, 20)
// MAGIC     plt.xlabel('Residuals')
// MAGIC     plt.ylabel('Frequency')
// MAGIC     plt.title(models[k], y=-.25)
// MAGIC
// MAGIC plt.show()
