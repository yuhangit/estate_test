package com.jiuzhong.utils

import com.jiuzhong.Estate.conf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// calculate host distribution for specific date and project scrape items
object HostDistribution {
    val nameArr = Array("scrip","ad","ts","url","ref","ua","dstip","cookie","src_port","json")
    val spark = SparkSession.builder().appName("OH!!").config(conf).getOrCreate()
    import spark.implicits._
    def main(args: Array[String]): Unit = {
        val path = args(0)
        val data = spark.read.format("csv").option("delimiter","\t").
            load(path).toDF(nameArr:_*)
        import scala.util.Try
        val getHostUDF = udf{ (url:String)=>
            Try{url.split("://")(1).split("/")(0)}.getOrElse(url)
        }
        data.select(getHostUDF($"url") as "host").groupBy("host").count.sort($"count".desc).show(false)
    }
}
