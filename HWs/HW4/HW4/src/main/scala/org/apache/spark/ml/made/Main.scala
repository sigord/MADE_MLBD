package org.apache.spark.ml.made

import breeze.{linalg => l}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{regression => r}
import org.apache.spark.sql.SparkSession


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
    val w = model.stages.last.asInstanceOf[r.LinearRegressionModel].coefficients

    //train our model

    df.show(5)
    println(w)
    //
    //    println(data)
    //    println("Hello world!")
    spark.stop()
  }
}