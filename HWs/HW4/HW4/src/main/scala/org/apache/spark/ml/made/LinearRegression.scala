package org.apache.spark.ml.made

import breeze.linalg.convert
import breeze.{linalg => l}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{DenseVector, Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.{BooleanParam, DoubleParam, IntParam, Param, ParamMap}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.SchemaUtils._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.{SparkContext, mllib}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}

trait LinearRegressionParams extends HasInputCol with HasOutputCol {
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  val n_steps = new IntParam(this, "steps", "Number of learning steps")
  def getSteps: Int = $(n_steps)
  def setSteps(value: Int): this.type = set(n_steps, value)

  val lr = new DoubleParam(this, "learningRate", "Learning rate")
  def getLR: Double = $(lr)
  def setLR(value: Double): this.type = set(lr, value)

  val eps = new DoubleParam(this, "eps", "Epsilon")
  def getEps: Double = $(eps)
  def setEps(value: Double): this.type = set(eps, value)

  setDefault(n_steps, 1000)
  setDefault(lr, 1e-1)
  setDefault(eps, 1e-4)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.fieldNames.contains($(outputCol))) {
      checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }


}

class LinearRegression(override val uid: String) extends Estimator[LinearRegressionModel]
  with LinearRegressionParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("linearRegression"))

  override def fit(dataset: Dataset[_]): LinearRegressionModel = {

    val sparkSession = SparkSession.builder.getOrCreate()

    implicit val encoder : Encoder[Vector] = ExpressionEncoder()

    val vectors: Dataset[Vector] = dataset.select(dataset($(inputCol)).as[Vector])

    val dim: Int = AttributeGroup.fromStructField(dataset.schema($(inputCol))).numAttributes.getOrElse(
      vectors.first().size
    )

    val w: l.DenseVector[Double] = l.DenseVector.zeros[Double](dim - 1)
    var bc_w = sparkSession.sparkContext.broadcast(w)
    println(w.length)

    //TODO Write fit function

    val steps: Int = getSteps
    val lr: Double = getLR
    val eps: Double = getEps
    val N: Double = vectors.count()
    val meanLR: Double = lr/N

    var gradientNorm: Double = 0.0

    for (i <- 1 to steps){
      var gradient = vectors.rdd.mapPartitions((partition: Iterator[Vector]) => {
        partition.map(row => {
          val trainVector: l.DenseVector[Double] = l.DenseVector[Double](row.toArray)
          val train_X = trainVector(0 to -2)
          val train_Y = trainVector(-1)
          (((train_X dot bc_w.value) - train_Y) * train_X) * 2.0
        })
      }).reduce((_+_))


      //update weights
      w -= meanLR * gradient
      bc_w = sparkSession.sparkContext.broadcast(w)
    }
    copyValues(new LinearRegressionModel(Vectors.fromBreeze(w))).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[LinearRegressionModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

object LinearRegression extends DefaultParamsReadable[LinearRegression]

class LinearRegressionModel private[made]
(
  override val uid: String,
  val weights: DenseVector
) extends Model[LinearRegressionModel] with LinearRegressionParams with MLWritable{

  private[made] def this(weights: Vector) =
    this(Identifiable.randomUID("linearRegressionModel"), weights.toDense)

  override def copy(extra: ParamMap): LinearRegressionModel = copyValues(
    new LinearRegressionModel(weights),
    extra
  )

  override def transform(dataset: Dataset[_]): DataFrame = {
    val bweights = weights.asBreeze.toDenseVector
    //TODO HOW FIX
    val coeff = Vectors.fromBreeze(bweights(0 to bweights.length -1))

    val transformUdf = dataset.sqlContext.udf.register(
      uid + "_predict",
      (x: Vector) => {x.dot(coeff)}
    )

    dataset.withColumn($(outputCol), transformUdf(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  override def write: MLWriter = new DefaultParamsWriter(this){
    override protected def saveImpl(path: String): Unit = {
      super.saveImpl(path)

      val result = Tuple1(weights.asInstanceOf[Vector])

      sqlContext.createDataFrame(Seq(result)).write.parquet(path + "/weights")
    }
  }
}

object LinearRegressionModel extends MLReadable[LinearRegressionModel]{
  override def read: MLReader[LinearRegressionModel] = new MLReader[LinearRegressionModel] {
    override def load(path: _root_.scala.Predef.String): _root_.org.apache.spark.ml.made.LinearRegressionModel = {

      val metadata = DefaultParamsReader.loadMetadata(path, sc)
      val result = sqlContext.read.parquet(path + "/weights")

      implicit  val encoder : Encoder[Vector] = ExpressionEncoder()

      val w = result.select(result("_1").as[Vector]).first()
      val model = new LinearRegressionModel(weights = w)
      metadata.getAndSetParams(model)
      model
    }
}
}

