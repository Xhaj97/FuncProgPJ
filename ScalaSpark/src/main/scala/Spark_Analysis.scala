import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
// << add this





object Spark_Analysis {
    def main(args: Array[String]): Unit = {

      val data = loadData()
      data.printSchema
      data.createOrReplaceTempView("patch")

      //data.show()


      var patchF = patchFail(data)
      var patchN = patchNorth(data)
      var patchS = patchSouth(data)
      var patchWNeg = patchWeatherNeg(data)
      var patchWMid = patchWeatherMid(data)
      var patchWPos = patchWeatherPos(data)
      var patchBat = patchBattery(data)
      var patchBatLow = patchBatteryLow(data)
      var patchBatMed = patchBatteryMed(data)
      var patchBatHigh = patchBatteryHigh(data)
      var patchGoodN = patchGoodNorth(data)
      var patchGoodS=patchGoodSouth(data)
      var patchGWNeg = patchGoodWNeg(data)
      var patchGWMed = patchGoodWMed(data)
      var patchGWHigh = patchGoodWHigh(data)




      patchStatut(data)
      println("Is there more failed Patch in the north or south Atmosphere?\n")
      println ("Number of failed Patch being in the north Atmosphere : " + patchN
        + " ( "+ (patchN*100)/patchGoodN + "% of the device in North )" )
      println ("Number of failed Patch being in the south Atmosphere : " + patchS
        + " ( "+ (patchS*100)/patchGoodS + "% of the device in South )\n" )

      println("Is there more failed device when the Weather is hot or cold?\n")
      println(" Cold")
      println("Number of failed patch while temperature is Cold (under 5 degree) :  " + patchWNeg
        + " ( "+ (patchWNeg*100)/patchF + "% of the failing device ) ")
      println("It represents "+ (patchWNeg*100)/patchGWNeg  + "% of the device in Cold temperature. " )
      println("\n  Medium")
      println("Number of failed patch while temperature is Medium (between 5 degree and 25 degree) :  " + patchWMid
        + " ( "+ (patchWMid*100)/patchF + "% of the failing device ) ")
      println("It represents "+ (patchWMid*100)/patchGWMed  + "% of the device in Medium temperature. " )
      println("\n Hot")
      println("Number of failed patch while temperature is Hot (more than 25 degree) :  " + patchWPos
        + " ( "+ (patchWPos*100)/patchF + "% of the failing device ) ")
      println("It represents "+ (patchWPos*100)/patchGWHigh  + "% of the device in Hot temperature. " )

      println("\nAmong the failed patch which percentage failed because of low battery?\n")
      println("Percentage of failed patch because of empty battery : " + (patchBat*100)/patchF  + "%")
      println("Percentage of failed patch because of low battery (under 20%) : " + (patchBatLow*100)/patchF +
        "% where " + (patchBat*100)/patchF + "% is because of empty battery." )
      println("Percentage of failed patch whose battery is between 20% and 75% : " + (patchBatMed*100)/patchF +"%")
      println("Percentage of failed patch whose battery is more than 75% : " + (patchBatHigh*100)/patchF +"%")

    }



    def loadData(): DataFrame = {
      val conf:SparkConf = new SparkConf().setAppName("Spark Analysis").setMaster("local[*]")
      val sc:SparkContext = new SparkContext(conf)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      sqlContext.read.json("your path/*.json")
    }

    def patchStatut(data:DataFrame):Unit = {
      data.groupBy("status").count().show()
    }
    def patchFail(data : DataFrame):Long = {
      data.filter(col("status") === "off").count()
    }

    def patchNorth(data:DataFrame):Long = {
      data.filter(col("status") === "off"
        && col("lat")>0)
        .count()
    }
    def patchSouth(data:DataFrame):Long = {
      data.filter(col("status") === "off"
        && col("lat")<=0)
        .count()
    }
    def patchWeatherNeg(data:DataFrame):Long ={
      data.filter(col("status") === "off"
        && col("temperature")<=5)
        .count()
    }
    def patchWeatherMid(data:DataFrame):Long ={
      data.filter(col("status") === "off"
        && col("temperature")>5
        && col("temperature")<=22)
        .count()
    }
    def patchWeatherPos(data:DataFrame):Long ={
      data.filter(col("status") === "off"
        && col("temperature")>22)
        .count()
    }

    def patchBattery(data:DataFrame):Long ={
      data.filter(col("status")==="off"
        && col("power")===0)
        .count()
    }

  def patchBatteryLow(data:DataFrame):Long={
    data.filter(col("status")==="off"
    && col("power") <=20)
    .count()
  }

  def patchBatteryMed(data:DataFrame):Long={
    data.filter(col("status")==="off"
      && col("power") >20
      && col("power")<=75)
      .count()
  }

  def patchBatteryHigh(data:DataFrame):Long={
    data.filter(col("status")==="off"
      && col("power") >75)
      .count()
  }

  def patchGoodNorth(data:DataFrame):Long={
    data.filter(col("lat")>0).count()
  }
  def patchGoodSouth(data:DataFrame):Long={
    data.filter(col("lat")<=0).count()
  }

  def patchGoodWNeg(data:DataFrame):Long={
    data.filter(col("temperature")<=5)
        .count()
    }

  def patchGoodWMed(data:DataFrame):Long={
    data.filter(col("temperature")>5
    && col("temperature")<= 25)
      .count()
  }

  def patchGoodWHigh(data:DataFrame):Long={
    data.filter(col("temperature")>25)
      .count()
  }





  }



