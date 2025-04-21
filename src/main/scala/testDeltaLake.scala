import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import io.delta.tables._
import org.apache.spark.sql.functions._

object testDeltaLake {
  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]").appName("Spark")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save("/tmp/delta-table")

    val df = spark.read.format("delta").load("/tmp/delta-table")
    df.show()

    val data1 = spark.range(5, 10)
    data1.write.format("delta").mode("overwrite").save("/tmp/delta-table")
    df.show()

    val deltaTable = DeltaTable.forPath("/tmp/delta-table")

    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100")))

    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    // Upsert (merge) new data
    val newData = spark.range(0, 20).toDF

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()


  }
}
