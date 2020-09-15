package wisardpkg.preprocess

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.{ParamMap, _}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Binarizer


/**
 * @param uid All types inheriting from `Identifiable` require a `uid`.
 *            This includes Transformers, Estimators, and Models.
 */
class Thermometer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol {
  /** @group param */
  val thermSize: IntParam =
    new IntParam(this, "thermSize", "thermometer size used to binarize continuous features")
  /** @group param */
  val thermMin: DoubleParam =
    new DoubleParam(this, "thermMin", "thermometer minimum used to binarize continuous features")
  /** @group param */
  val thermMax: DoubleParam =
    new DoubleParam(this, "thermMax", "thermometer maximum used to binarize continuous features")

  def this() = this(Identifiable.randomUID("binarizer"))

  /** @group setParam */
  def setThermSize(value: Int): this.type = set(thermSize, value)

  /** @group setParam */
  def setThermMin(value: Double): this.type = set(thermMin, value)

  /** @group setParam */
  def setThermMax(value: Double): this.type = set(thermMax, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(inputCol -> "X", outputCol -> "X_therm", thermSize -> 256)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val (inputColName, outputColName) = ($(inputCol), $(outputCol))

    val thermometerUDF = dataset.schema(inputColName).dataType match {
      case DoubleType =>
        udf { in: Double =>
          val tds = breeze.linalg.linspace($(thermMin), $(thermMax), $(thermSize))
          tds.map(x => if (in < x) 0 else 1).toArray
        }
    }

    val ouputCol = thermometerUDF(col(inputColName))

    dataset.withColumn(outputColName, ouputCol)
  }


  override def transformSchema(schema: StructType): StructType = {
    var outputFields = schema.fields
    val inputType = schema($(inputCol)).dataType
    val outputField = inputType match {
      case DoubleType =>
        DataTypes.createStructField($(outputCol), DataTypes.StringType, false)
      case _ =>
        throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }
    outputFields :+= outputField
    StructType(outputFields)
  }

  /**
   * Creates a copy of this instance.
   *
   * @param extra Param values which will overwrite Params in the copy.
   */
  def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ThermometerExample {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)

    // Configure Spark
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark: SparkSession = SparkSession
      .builder
      .config(sparkConf)
      .appName("ThermometerExample")
      .getOrCreate()

    // Read csv file
    val schema: StructType = new StructType()
      .add("label", IntegerType, nullable = true)
      .add("X", DoubleType, nullable = true)
    val inputDataFrame: DataFrame = spark.sqlContext.read.format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema)
      .load(inputPath)
      //      .load("/home/joao/Documents/Big Data/dac/train.txt")
      .na.drop()

    inputDataFrame.show(10)

    val takes = Array(10, 100, 1000, 10000, 100000, 200000)

    // Binarizer
    spark.sparkContext.setJobGroup("0", "Binarizer")
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("X")
      .setOutputCol("X_bin")
      .setThreshold(0.5)
    val binarizedDataFrame: DataFrame = binarizer.transform(inputDataFrame)
    println(s"-- Binarizer --")
    takes.foreach{t =>
      println(s"- take(${t}) -")
      spark.time(binarizedDataFrame.take(t))
    }
    println("")

    // Thermometer
    val sizes = Array(64, 128, 256, 512, 1024, 2048)
    takes.foreach{ t =>
      println(s"-- take(${t}) --")
      sizes.foreach { s =>
        spark.sparkContext.setJobGroup(s"1", s"Thermometer(${s})")
        val thermometer: Thermometer = new Thermometer("thermometer1")
          .setInputCol("X")
          .setOutputCol("X_therm")
          .setThermSize(s)
          .setThermMin(-100.0)
          .setThermMax(100.0)
        val thermometerDataFrame: DataFrame = thermometer.transform(inputDataFrame)
        println(s"- Thermometer ${s} bits -")
        spark.time(thermometerDataFrame.take(t))
        println("")
      }
      println("")
      println("")
    }

    //    Thread.sleep(1000000) //For 1000 seconds or more

    spark.stop()
  }
}