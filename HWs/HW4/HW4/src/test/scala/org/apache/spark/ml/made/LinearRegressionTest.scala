package org.apache.spark.ml.made

import com.google.common.io.Files
import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.ml
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, SparkSession}

class LinearRegressionTest extends AnyFlatSpec with should.Matchers with  WithSpark {
  val delta = 0.0001
  lazy val data: DataFrame = LinearRegressionTest._data
  lazy val vectors: Seq[Vector] = LinearRegressionTest._vectors
  lazy val weights: Vector = LinearRegressionTest._weights

  "Model" should "predict (transform) with weights" in {
    val model: LinearRegressionModel = new LinearRegressionModel(
      weights = weights
    ).setInputCol("features").setOutputCol("target")

    validateModel(model, model.transform(data))
  }

  "Estimator" should "calculate true weights" in {
    val estimator = new LinearRegression()
      .setInputCol("features").setOutputCol("target")
  }

  private def validateModel(model: LinearRegressionModel, data: DataFrame) = {
    val vectors: Array[Double] = data.collect().map(_.getAs[Double]("target"))

    vectors.length should be(3)

    vectors(0) should be(182.47324376 +- delta)
    vectors(1) should be(-195.6666581 +- delta)
    vectors(2) should be(48.11468055 +- delta)
  }
}




object LinearRegressionTest extends WithSpark {
  lazy val _vectors = Seq(
    Vectors.dense(1.0831739 ,  0.24972623,  0.35019626,  0.95278281),
    Vectors.dense(-0.83118126,  1.09104714, -2.04925277, -1.06971245),
    Vectors.dense(1.82273516, -2.70778954,  1.07529455,  1.34053698)
  )

  lazy val _weights = Vectors.dense(46.82544441, 87.59731255, 77.86859935, 86.70234718)

  lazy val _data: DataFrame = {
    import sqlc.implicits._
    _vectors.map(x => Tuple1(x)).toDF("features")
  }
}