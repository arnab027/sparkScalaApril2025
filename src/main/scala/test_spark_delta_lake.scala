import org.apache.spark.sql.SparkSession
import org.apache.spark.types
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, expr}

object test_spark_delta_lake {
  def main(args:Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("DeltaLakeOperations")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val sc = spark.sparkContext

    //read json file into dataframe
    val df = spark.read.json("C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\main\\scala\\zipcodes.json")
    df.printSchema()
    df.show(false)

    //read multiline json file
    val multiline_df = spark.read.option("multiline", "true")
      .json("C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\main\\scala\\multiline-zipcode.json")
    multiline_df.printSchema()
    multiline_df.show(false)


    //read multiple files
//    val df2 = spark.read.json(
//      "src/main/resources/zipcodes_streaming/zipcode1.json",
//      "src/main/resources/zipcodes_streaming/zipcode2.json")
//    df2.show(false)
//
//    //read all files from a folder
//    val df3 = spark.read.json("src/main/resources/zipcodes_streaming/*")
//    df3.show(false)

    //Define custom schema
    val schema = new StructType()
      .add("City", StringType, true)
      .add("Country", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("EstimatedPopulation", LongType, true)
      .add("Lat", DoubleType, true)
      .add("Location", StringType, true)
      .add("LocationText", StringType, true)
      .add("LocationType", StringType, true)
      .add("Long", DoubleType, true)
      .add("Notes", StringType, true)
      .add("RecordNumber", LongType, true)
      .add("State", StringType, true)
      .add("TaxReturnsFiled", LongType, true)
      .add("TotalWages", LongType, true)
      .add("WorldRegion", StringType, true)
      .add("Xaxis", DoubleType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("Zipcode", StringType, true)
      .add("ZipCodeType", StringType, true)

    val df_with_schema = spark.read.schema(schema).json("C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\main\\scala\\zipcodes.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)

    df_with_schema.createOrReplaceTempView("test_view")
    spark.sql("SELECT * from test_view").show()

    df_with_schema.write.format("delta").mode("append").save("C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\tmp\\output\\delta_table")
    val deltaTableInstance = DeltaTable.forPath(spark, "C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\tmp\\output\\delta_table")
//    condition = expr("id % 2 == 0"),
//    set = Map("id" -> expr("id + 100"))

//    deltaTable.update(
//      condition = expr("id % 2 == 0"),
//      set = Map("id" -> expr("id + 100")))

    deltaTableInstance.update(
                                condition = expr("City == 'HOLDER'"),
                                    set =
                                      Map("Zipcode" -> expr("'111111111'"),
                                           "ZipCodeType" -> expr("'Arnab'")
                                                                        ))

    val df_with_schema_1 = spark.read.format("delta").load("C:\\Users\\arnab\\IdeaProjects\\scalaSparkApril2025\\src\\tmp\\output\\delta_table")
    val filter_df = df_with_schema_1.filter(col("City").like("HOLDER"))
    filter_df.show()
    //Write json file

//    df2.write
//      .json("/tmp/spark_output/zipcodes1.json")
  }

}
