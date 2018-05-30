package com.jiuzhong.scraper

import com.jiuzhong.Enc
import com.jiuzhong.utils.URLRecord
import com.jiuzhong.utils.ColumnAdd
import com.jiuzhong.utils.utils.{deleteGolbalFile, readData, writeData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{size, udf}

class ScrapeCDPI(spark:SparkSession,configMap:Map[String,String]) {
    val todayStr = configMap("todayStr")
    val statArr = Array("mdn","tag","counts","id")
    val userUrlArr = Array("imsi","mdn","meid","nai","destinationip","destinationport","starttime","endtime","download_bytes","upload_bytes",
        "destinationurl","protocolid","service_option","useragent","referer","bsid","host","datevalue","hour_zone","site","refer_site",
        "refer_host","userkw_single","cookie","source_ip","source_port","filename","datelabel","loadstamp")
    val cdpiArr = Array("imsi","mdn","destinationurl","useragent","referer","cookie","datelabel","loadstamp","source")
    val sc = spark.sparkContext
    import spark.implicits._
    def scrape(dateStr:String,enc:Enc):Unit ={
        val urlsRDD= sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "")
        urlsRDD.saveAsTextFile(configMap("urlPath")+"asda")
        val urls = spark.read.format("json").load(configMap("urlPath") +"asda").as[URLRecord].cache()
        urls.createTempView("urls")
        urls.count()
        deleteGolbalFile(configMap("urlPath") +"asda")
        val hosts = urls.map(_.host).distinct().collect()
        val stmt =
            s"""
               |select imsi,mdn,destinationurl,useragent,referer,cookie,datelabel,loadstamp, "userurl" as source
               |from db_cdpi.sada_cdpi_userurl where datelabel rlike '${dateStr}'
               |union all
               |select imsi,mdn,destinationurl, null ,null , null, datelabel, loadstamp, "userflow" as source
               |from db_cdpi.sada_cdpi_userflow where datelabel rlike '${dateStr}'
             """.stripMargin

        val srcData = spark.sql(stmt)

        val filterCol = udf{(url:String) =>
            if (url == null) false
            else
                hosts.exists(url.contains(_))
        }
        //
        //        val saveTbl = srcData.as("t1").join(urls.as("t2"),instrCol($"t1.url",$"t1.ref",$"t2.cookie", $"t2.url",$"t2.ref",$"t2.cookie"), "leftsemi")
        srcData.where(filterCol($"destinationurl"))
            .createTempView("source_data")

        val data  = spark.sql("""select t1.* from source_data t1
                             where exists
                                (select 1 from urls t2
                                where inString(t1.destinationurl,t2.url)
                                    and inString(t1.cookie, t2.cookie)
                                    and inString(t1.referer,t2.ref)
                                )""")
        //        val cnt = (data.count()/10000).toInt
        writeData(data,configMap("scrapePath") )
    }
    def process(enc:Enc): Unit ={
        val process = spark.read.format("csv").option("delimiter","\t").load(configMap("scrapePath")).toDF(cdpiArr:_*)
        process.createTempView("process")

        val tags = sc.textFile(configMap("tagPath")).map(l => enc.decrypt(l)).filter(_.trim() != "").map( l => (l.split("\\s+")(0),l.split("\\s+")(1))).toDF("tag","appid")
        tags.createTempView("tags")
        val lte_process_count = configMap("lte_process_count")
        val res = spark.sql("""select mdn,collect_list(tags.tag) as tag, collect_set(tags.tag) as counts,
                               row_number() over (order by mdn) as id
                        from  process inner join tags on instr(destinationurl,appid) > 0
                        group by mdn
                         """)
            .withColumn("tag", $"tag" mkString ",").withColumn("counts", size($"counts"))
            .where(s"counts >= $lte_process_count")


        writeData(res,configMap("processPath"),10)
    }

    def dropHistory(): Unit ={
        val dropHis = spark.read.format("csv").option("delimiter","\t").load(configMap("processPath")).toDF(statArr:_*)
        val history = readData(spark, configMap("historyPath")).toDF("phone")
        dropHis.createTempView("dropHis")
        history.createTempView("history")

        val filter = spark.sql(
            """
              |select dropHis.*
              |from dropHis
              |where not exists(
              |  select 1
              |  from history
              |  where drophis.mdn = history.phone
              |)
            """.stripMargin
        )
        writeData(filter,configMap("dropHistoryPath"))
    }

    def kv(key:String):Unit= {
        val clientID = configMap("clientID")
        val requirementID = key.split("_")(0)
        val serialID = key.split("_")(1)
        val inDF = spark.read.format("csv").option("delimiter","\t").option("inferschema",true)
            .load(configMap("dropHistoryPath")).toDF(statArr:_*)

        inDF.createTempView("inDF")
        val kv = spark.sql(
            s"""
               |select concat(concat_ws('_','$clientID','$requirementID','$todayStr','$serialID',
               |                     row_number() over (order by id) -1),'\t') as key,
               |       concat_ws('\t',format_string('%04d',id), mdn) as value
               |
              |from inDF
            """.stripMargin)
        //        select(lit(key + underScore + todayStr + underScore), row_number().over(Window.orderBy("tag") )
        //            ,concat(lit(ad + delm),$"tag" ,lit(delm), $"mobile")).toDF("k1","k2","value")
        //            .withColumn("",concat( $"k1",$"k2" - 1))
        //            .select("key","value")

        kv.createTempView("kv")
        kv.show(false)
        val kvTbl = spark.sql(
            s"""
               |select concat_ws("\t",key,value)
               |from
               |    (select key,value from kv
               |    union
               |    select concat_ws('_','$clientID','$requirementID','$todayStr','$serialID','total') , count(*)
               |    from kv)
            """.stripMargin)
        val historyDF = inDF.select("mdn")
        writeData(historyDF,configMap("saveHistoryPath"),1)
        kvTbl.coalesce(1).write.format("csv").option("delimiter","\u0001")
            .option("ignoreTrailingSpace",false).save(configMap("kvPath"))
    }

}
