package com.jiuzhong.scraper

import com.jiuzhong.Enc
import com.jiuzhong.utils.URLRecord
import com.jiuzhong.utils.ColumnAdd
import com.jiuzhong.utils.utils.{deleteGolbalFile, readData, writeData}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
//        urls.show(50,false)
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
        val process = readData(spark,configMap("scrapePath")).toDF(processArr:_*)
        process.createTempView("process")

        val tags = sc.textFile(configMap("tagPath")).map(l => enc.decrypt(l)).filter(_.trim() != "").map{
            l =>
                var arr = l.split("\\s+")
                (arr(0),arr(1),arr(2))
        }.toDF("tag","appid","id")
        tags.createTempView("tags")
        val lte_process_count = configMap("lte_process_count")
        val res = spark.sql("""select msisdn,collect_list(tags.tag) as tag, collect_set(tags.tag) as counts,
                               collect_set(tags.id) as id
                        from  process inner join tags on instr(url,appid) > 0
                        group by msisdn
                         """)
            .withColumn("tag", $"tag" mkString ",").withColumn("counts", size($"counts"))
            .withColumn("id", $"id" mkString "_")
            .where(s"counts >= $lte_process_count")
        res.createTempView("lte_res")

        // add group contain process
        if (configMap.getOrElse("lte_group_process", "0") == "1"){
        }

        val verifyPhonePath = "/user/g_tel_hlwb_data/public/hlwb_data1/phone_set/"
        val verifyPhone = spark.read.textFile(verifyPhonePath)
        verifyPhone.createTempView("filter_phone")
        val result_verify = spark.sql(
            """
              |select lte_res.*
              |from lte_res
              |where exists (
              |     select 1
              |     from filter_phone
              |     where filter_phone.value = lte_res.msisdn
              |)
            """.stripMargin)

        writeData(result_verify,configMap("processPath"),10)
    }

    def dropHistory(): Unit ={
        val dropHis = readData(spark,configMap("processPath")).toDF(statArr:_*)
        var filter:DataFrame = null
        try{
            val history = readData(spark, configMap("historyPath")).toDF("phone")
            dropHis.createTempView("dropHis")
            history.createTempView("history")

            filter = spark.sql(
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
        }
        catch {
            case e: java.lang.IllegalArgumentException =>
                filter = dropHis;
            case e:Throwable => throw e;
        }

        writeData(filter,configMap("dropHistoryPath"))
    }

    def kv(key:String):Unit= {
        val clientID = configMap("clientID")
        val requirementID = key.split("_")(0)
        val serialID = key.split("_")(1)
        val keySuff = key.split("_").drop(2).mkString("_")

        val inDF = readData(spark,configMap("dropHistoryPath")).toDF(statArr:_*)

        inDF.createTempView("inDF")
        val kv = spark.sql(
            s"""
               |select concat(concat_ws('_','$clientID','$requirementID','$todayStr','$serialID',
               |                     row_number() over (order by id) -1),'\t') as key,
               |       concat_ws('\t',concat_ws("_", row_number() over (order by id) -1),id ,'$keySuff') , msisdn) as value
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
