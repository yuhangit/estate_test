package com.jiuzhong.scraper

import com.jiuzhong.Enc
import com.jiuzhong.utils.URLRecord
import com.jiuzhong.utils.utils.{deleteGolbalFile, readData, writeData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{size, udf}

class ScrapeLTE(spark: SparkSession,configMap:Map[String,String]) extends java.io.Serializable {
    import spark.implicits._
    val statArr = Array("msisdn","tag","counts","id")
    val processArr = Array("start_time","end_time","imsi","msisdn","apn","tai","ecgi","rat_type","user_ip","server_ip","user_port",
        "server_port","ip_protocol","s_gw_ip", "p_gw_ip","up_bytes","down_bytes","up_packets","down_packet","protocol_category",
        "application","sub_appliction","egn_sub_protocol","url","user_agent", "referer","cookie","imsisv","dt","hour")
    private val sc = spark.sparkContext
    val todayStr = configMap("todayStr")

    def scrape(dateStr:String,enc:Enc): Unit = {

        val urlsRDD= sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "")
        urlsRDD.saveAsTextFile(configMap("urlPath")+"asda")
        val urls = spark.read.format("json").load(configMap("urlPath") +"asda").as[URLRecord].cache()
        urls.createTempView("urls")
        urls.count()
        deleteGolbalFile(configMap("urlPath") +"asda")
        val hosts = urls.map(_.host).distinct().collect()
        val stmt = s"select * from db_lte.sada_lte_ufdr where dt rlike '${dateStr}'"

        val srcData = spark.sql(stmt)

        val filterCol = udf{(url:String) =>
            if (url == null) false
            else
                hosts.exists(url.contains(_))
        }
        //
        //        val saveTbl = srcData.as("t1").join(urls.as("t2"),instrCol($"t1.url",$"t1.ref",$"t2.cookie", $"t2.url",$"t2.ref",$"t2.cookie"), "leftsemi")
        srcData.where(filterCol($"url"))
            .createTempView("source_data")

        val data  = spark.sql("""select t1.* from source_data t1
                             where exists
                                (select 1 from urls t2
                                where inString(t1.url,t2.url)
                                    and inString(t1.cookie, t2.cookie)
                                    and inString(t1.referer,t2.ref)
                                )""")
        //        val cnt = (data.count()/10000).toInt
        writeData(data,configMap("scrapePath") )
    }

    def process(enc: Enc): Unit ={
        val process = spark.read.format("csv").option("delimiter","\t").load(configMap("scrapePath")).toDF(processArr:_*)
        process.createTempView("process")

        val tags = sc.textFile(configMap("tagPath")).map(l => enc.decrypt(l)).filter(_.trim() != "").map( l => (l.split("\\s+")(0),l.split("\\s+")(1))).toDF("tag","appid")
        tags.createTempView("tags")
        val res = spark.sql("""select msisdn,collect_list(tags.tag) as tag, collect_set(tags.tag) as counts,
                               row_number() over (order by msisdn) as id
                        from  process inner join tags on instr(url,appid) > 0
                        group by msisdn
                         """)
            .withColumn("tag", $"tag" mkString ",").withColumn("counts", size($"counts"))
            .where("counts >= 3")


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
              |  where drophis.msisdn = history.phone
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
               |       concat_ws('\t',format_string('%04d',id), msisdn) as value
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
        val historyDF = inDF.select("msisdn")
        writeData(historyDF,configMap("saveHistoryPath"),1)
        kvTbl.coalesce(1).write.format("csv").option("delimiter","\u0001")
            .option("ignoreTrailingSpace",false).save(configMap("kvPath"))
    }

}
