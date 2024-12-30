import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BitcoinDataProcessing {

  // Function to read the CSV data
  private def readData(filePath: String)(implicit spark: SparkSession): DataFrame = {
    try {
      val data = spark.read.option("header", "true").csv(filePath)
      println("Data loaded successfully!")
      data
    } catch {
      case e: Exception =>
        println(s"Error loading data: ${e.getMessage}")
        null
    }
  }

  // Function to convert timestamp to readable datetime
  def convertTimestampToDatetime(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    try {
      val dataWithDatetime = data.withColumn("Datetime", from_unixtime(col("Timestamp").cast("long")))
      println("Timestamp converted to datetime!")
      dataWithDatetime
    } catch {
      case e: Exception =>
        println(s"Error converting timestamp: ${e.getMessage}")
        data
    }
  }

  // Function to set the datetime as index (not directly applicable in Spark, but we'll sort by Datetime)
  def setDatetimeAsIndex(data: DataFrame): DataFrame = {
    try {
      val dataWithIndex = data.orderBy("Datetime") // Spark doesn't have an index like pandas
      println("Datetime set as index!")
      dataWithIndex
    } catch {
      case e: Exception =>
        println(s"Error setting datetime as index: ${e.getMessage}")
        data
    }
  }

  // Function to drop a specified column
  def dropColumn(data: DataFrame, columnName: String): DataFrame = {
    try {
      val dataWithoutColumn = data.drop(columnName)
      println(s"Column '$columnName' dropped!")
      dataWithoutColumn
    } catch {
      case e: Exception =>
        println(s"Error dropping column '$columnName': ${e.getMessage}")
        data
    }
  }

  // Function to fill missing values using forward-fill method
  def fillMissingValues(data: DataFrame): DataFrame = {
    try {
      // Forward filling requires a Window spec to fill missing values
      val windowSpec = Window.orderBy("Datetime").rowsBetween(-Window.unboundedPreceding, Window.currentRow)
      val dataWithFilledValues = data.na.fill("ffill") // Spark has a limited `fill` for continuous rows.
      println("Missing values filled using forward-fill!")
      dataWithFilledValues
    } catch {
      case e: Exception =>
        println(s"Error filling missing values: ${e.getMessage}")
        data
    }
  }

  // Function to calculate price range (High - Low)
  def calculatePriceRange(data: DataFrame): DataFrame = {
    try {
      val dataWithPriceRange = data.withColumn("Price_Range", col("High") - col("Low"))
      println("Price range calculated!")
      dataWithPriceRange
    } catch {
      case e: Exception =>
        println(s"Error calculating price range: ${e.getMessage}")
        data
    }
  }

  // Function to calculate moving averages
  def calculateMovingAverage(data: DataFrame, windowSize: Int, columnName: String): DataFrame = {
    try {
      val movingAverageCol = s"MA_$columnName_$windowSize"
      val dataWithMovingAvg = data.withColumn(movingAverageCol, avg(col(columnName)).over(Window.orderBy("Datetime").rowsBetween(-windowSize + 1, 0)))
      println(s"$windowSize-period moving average of $columnName calculated!")
      dataWithMovingAvg
    } catch {
      case e: Exception =>
        println(s"Error calculating moving average: ${e.getMessage}")
        data
    }
  }

  // Function to calculate daily returns
  def calculateDailyReturns(data: DataFrame): DataFrame = {
    try {
      val dataWithDailyReturns = data.withColumn("Daily_Return", (col("Close") / lag(col("Close"), 1).over(Window.orderBy("Datetime")) - 1) * 100)
      println("Daily returns calculated!")
      dataWithDailyReturns
    } catch {
      case e: Exception =>
        println(s"Error calculating daily returns: ${e.getMessage}")
        data
    }
  }

  // Function to add a column indicating if the Close price increased or decreased
  def calculateCloseIncreased(data: DataFrame): DataFrame = {
    try {
      val dataWithCloseIncreased = data.withColumn("Close_Increased", when(col("Close") > lag(col("Close"), 1).over(Window.orderBy("Datetime")), 1).otherwise(0))
      println("Close increased/decreased column added!")
      dataWithCloseIncreased
    } catch {
      case e: Exception =>
        println(s"Error calculating Close_Increased: ${e.getMessage}")
        data
    }
  }

  // Function to resample the data to daily frequency and calculate mean of Close price
  def resampleDataToDaily(data: DataFrame): DataFrame = {
    try {
      val dailyData = data.groupBy(window(col("Datetime"), "1 day")).agg(mean("Close").alias("Daily_Close_Mean"))
      println("Data resampled to daily frequency!")
      dailyData
    } catch {
      case e: Exception =>
        println(s"Error resampling data: ${e.getMessage}")
        null
    }
  }

  // Function to calculate cumulative volume
  def calculateCumulativeVolume(data: DataFrame): DataFrame = {
    try {
      val dataWithCumulativeVolume = data.withColumn("Cumulative_Volume", sum("Volume").over(Window.orderBy("Datetime")))
      println("Cumulative volume calculated!")
      dataWithCumulativeVolume
    } catch {
      case e: Exception =>
        println(s"Error calculating cumulative volume: ${e.getMessage}")
        data
    }
  }

  // Function to check data types of columns
  def checkDataTypes(data: DataFrame): Unit = {
    try {
      data.printSchema()
    } catch {
      case e: Exception =>
        println(s"Error checking data types: ${e.getMessage}")
    }
  }

  // Main function to execute all steps
  def processData(filePath: String)(implicit spark: SparkSession): DataFrame = {
    val data = readData(filePath)
    if (data != null) {
      var transformedData = data
      transformedData = convertTimestampToDatetime(transformedData)
      transformedData = setDatetimeAsIndex(transformedData)
      transformedData = dropColumn(transformedData, "Timestamp")
      transformedData = fillMissingValues(transformedData)
      transformedData = calculatePriceRange(transformedData)
      transformedData = calculateMovingAverage(transformedData, 10, "Close")
      transformedData = calculateMovingAverage(transformedData, 30, "Close")
      transformedData = calculateDailyReturns(transformedData)
      transformedData = calculateCloseIncreased(transformedData)
      val dailyData = resampleDataToDaily(transformedData)
      transformedData = calculateCumulativeVolume(transformedData)
      checkDataTypes(transformedData)

      // Return the final transformed data
      transformedData
    } else {
      null
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder
      .appName("Bitcoin Data Processing")
      .master("local[*]") // or any other cluster settings
      .getOrCreate()

    val filePath = "\"D:\\Workspace\\BitcoinProject\\assets\\bitcoin_data.csv\""
    val processedData = processData(filePath)

    if (processedData != null) {
      processedData.show(5)
    }

    transformData(processedData)
    calculateAveragePrices(processedData)
    calculateMovingAverage(processedData)
    spark.stop()
  }

  // Function to transform data (filter and add new columns)
  def transformData(df: DataFrame): DataFrame = {
    df.filter("Volume > 0") // Filter out rows with Volume = 0
      .withColumn("Date", from_unixtime(col("Timestamp")).cast("timestamp")) // Convert Timestamp to Date

    print(s"New Field added ${df.show(5)}")

    df
  }

  // Function to calculate average prices per day
  def calculateAveragePrices(df: DataFrame): DataFrame = {
    println("called: calculateAveragePrices")
    df.groupBy("Date")
      .agg(
        avg("Open").alias("avg_open"),
        avg("High").alias("avg_high"),
        avg("Low").alias("avg_low"),
        avg("Close").alias("avg_close")
      )
  }

  // Function to calculate a moving average (5-period moving average example)
  def calculateMovingAverage(df: DataFrame): DataFrame = {
    println("called: calculateMovingAverage")
    val windowSpec = Window.orderBy("Date").rowsBetween(-4, 0)  // 5-period moving average
    df.withColumn("moving_avg_close", avg("Close").over(windowSpec))
  }
}
