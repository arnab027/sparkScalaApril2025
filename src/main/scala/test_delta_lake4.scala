import org.apache.spark.sql.SparkSession

object test_delta_lake4 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    // The 'appName' is a name for your application, visible in the Spark UI.
    // The 'master' specifies the master URL for connecting to a cluster.
    // "local[*]" means run Spark locally with as many worker threads as logical cores.
    val spark = SparkSession.builder
      .appName("SimpleSparkApp")
      .master("local[*]")
      .getOrCreate()

    // Create a simple DataFrame
    val data = Seq(("Alice", 1), ("Bob", 2), ("Charlie", 3))
    val df = spark.createDataFrame(data).toDF("Name", "ID")

    // Show the DataFrame
    println("Original DataFrame:")
    df.show()

    // Perform a simple transformation: filter rows where ID > 1
    val filteredDf = df.filter(df("ID") > 1)

    // Show the transformed DataFrame
    println("Filtered DataFrame (ID > 1):")
    filteredDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}