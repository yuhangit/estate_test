package com.jiuzhong


import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object Estate {
  val  spark = SparkSession.builder().appName("Estate").getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  val today = new SimpleDateFormat("yyMMdd").format(new java.util.Date())

  def scrapeSource(dt:String, enc: Enc): Unit ={
    val publicPath = "hdfs://ns1/user/gdpi/public"
    val addcookiePath = s"${publicPath}/sada_gdpi_adcookie/${dt}/*/*.gz"
    val newclickPath = s"${publicPath}/sada_new_click/${dt}/*/*.gz"
//    val postPath = s"${publicPath}/sada_gdpi_post_click/${dt}/*/*.gz"

    val source = sc.textFile("%s,%s".format(addcookiePath,newclickPath))
//    val source = sc.textFile("%s,%s".format(addcookiePath,newclickPath,postPath))

    val urlPath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/estate/config/url.txt"
    val savePath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/estate/${dt}/scrapeSource"

    val urls = sc.broadcast(sc.textFile(urlPath).map(l => enc.decrypt(l).split(" +")).collect().toList)

    val saveTbl = source.filter{
      record =>
        val arr = record.split("\t")
        val sourceurl = arr(3)
        val refurl = arr(4)
        urls.value.exists(url => url.forall(part => sourceurl.contains(part) || refurl.contains(part)))
    }.toDF()

    saveTbl.coalesce(1000).write.format("com.databricks.spark.csv").
      option("delimiter","\t").save(savePath)
  }

  def main(args: Array[String]): Unit = {
    val dateStr = args(0)
    // don't initial class in class body
    val enc = new Enc()
    scrapeSource(dateStr,enc)
  }
}
