package com.jiuzhong


import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

case class SadaRecord(scrip:String, ad:String, ts:String, url:String, ref:String, ua:String, dstip:String,cookie:String,
                      srcPort:String)
object Estate {
  val  spark = SparkSession.builder().appName("Estate").getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext
  val today = new SimpleDateFormat("yyMMdd").format(new java.util.Date())

  def scrapeSource(prjName:String, dt:String, enc: Enc): Unit ={
    val publicPath = "hdfs://ns1/user/gdpi/public"
    val addcookiePath = s"${publicPath}/sada_gdpi_adcookie/${dt}/*/*.gz"
    val newclickPath = s"${publicPath}/sada_new_click/${dt}/*/*.gz"
//    val postPath = s"${publicPath}/sada_gdpi_post_click/${dt}/*/*.gz"

    val source = sc.textFile("%s,%s".format(addcookiePath,newclickPath))
//    val source = sc.textFile("%s,%s".format(addcookiePath,newclickPath,postPath))
    val saveBase = "hdfs://ns1/user/u_tel_hlwb_mqj/private/test"
    val urlPath = s"${saveBase}/config/${prjName}_url.txt"
    val savePath = s"${saveBase}/${prjName}/${dt}/scrapeSource"
    val urls_encrypt = sc.textFile(urlPath).map(l => enc.decrypt(l).split(" +"))

    val urls = sc.broadcast(urls_encrypt.collect().toList)

    val urls_decrypt =s"${saveBase}/${prjName}/config/${prjName}_url.txt"
    urls_encrypt.map(l => l.mkString(" ")).saveAsTextFile(urls_decrypt)


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
    val prjName = args(0)
    val dateStr = args(1)
    // don't initial class in class body
    val enc = new Enc()
    scrapeSource(prjName,dateStr,enc)
  }
}
