package org.apache.spark.ml.made

import breeze.{linalg => l}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{regression => r}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    //start spark session
    val spark:SparkSession = SparkSession.builder
      .master("local[4]")
      .appName("HW4")
      .getOrCreate()

    import spark.implicits._

    //generate data
    val X: l.DenseMatrix[Double] = l.DenseMatrix.rand[Double](100000, 3)
    val y: l.DenseVector[Double] = X * l.DenseVector[Double](1.5,0.3,-0.7)

    val data = l.DenseMatrix.horzcat(X, y.asDenseMatrix.t)
    val df = data(l.*, ::).iterator
      .map(x => (x(0), x(1), x(2), x(3)))
      .toSeq.toDF("x1","x2","x3","y")

    //train default model
    val pipeline = new Pipeline().setStages(Array(
      new VectorAssembler()
        .setInputCols(Array("x1", "x2", "x3"))
        .setOutputCol("features"),
      new r.LinearRegression().setLabelCol("y")
    ))

    val model = pipeline.fit(df)
    val w_default = model.stages.last.asInstanceOf[r.LinearRegressionModel].coefficients

    //train our model
    val ourPipline = new Pipeline().setStages(Array(
      new VectorAssembler()
        .setInputCols(Array("x1", "x2", "x3", "y"))
        .setOutputCol("features"),
      new LinearRegression().setInputCol("features").setOutputCol("target")
    ))

    val ourModel = ourPipline.fit(df)
    val w_our = ourModel.stages.last.asInstanceOf[LinearRegressionModel].weights

    println("Default model weights:", w_default)
    println("Our model weights:", w_our)


    val features_df: DataFrame = X(l.*, ::).iterator
      .map(x => (x(0), x(1), x(2)))
      .toSeq.toDF("x1", "x2", "x3")
    val formatter = new VectorAssembler()
      .setInputCols(Array("x1", "x2", "x3"))
      .setOutputCol("features")
    val features_X = formatter.transform(features_df)


    val predictionModel: LinearRegressionModel = new LinearRegressionModel(w_our)
      .setInputCol("features").setOutputCol("target")
    val pred = predictionModel.transform(features_X)
    val vectors: Array[Double] = pred.collect().map(_.getAs[Double]("target"))


    println("Original target",  y(0 to 10))
    println("Predicted target", l.DenseVector[Double](vectors.slice(0, 10).toArray))
    spark.stop()
  }
}