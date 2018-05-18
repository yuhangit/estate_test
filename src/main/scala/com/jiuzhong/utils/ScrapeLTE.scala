package com.jiuzhong.utils

import com.jiuzhong.Enc
import com.jiuzhong.utils.utils.{deleteGolbalFile, writeData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

class ScrapeLTE(spark: SparkSession) {
    import spark.implicits._
    val processArr = Array("start_time","end_time","imsi","msisdn","apn","tai","ecgi","rat_type","user_ip","server_ip","user_port",
        "server_port","ip_protocol","s_gw_ip", "p_gw_ip","up_bytes","down_bytes","up_packets","down_packet","protocol_category",
        "application","sub_appliction","egn_sub_protocol","url","user_agent", "referer","cookie","imsisv","dt","hour")
    private val sc = spark.sparkContext
    def scrape(configMap: Map[String, String], dateStr :String,enc: Enc): Unit = {

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

    def process(cfgs:Map[String,String],enc: Enc): Unit ={
        val process = spark.read.format("csv").option("delimiter","\t").load(cfgs("scrapePath")).toDF(processArr:_*)
        process.createTempView("process")

        val tags = sc.textFile(cfgs("tagPath")).map(l => enc.decrypt(l)).filter(_.trim() != "").map( l => (l.split("\\s+")(0),l.split("\\s+")(1))).toDF("tag","appid")
        tags.createTempView("tags")
        tags.show(false)
        val res = spark.sql("""select msisdn,collect_list(tags.tag) as tag
                        from  process inner join tags on instr(url,appid) > 0
                        group by msisdn
                         """)
            .withColumn("tag", $"tag" mkString ",")

        writeData(res,cfgs("processPath"),10)
    }


    def dropHistory(cfgs:Map[String,String]): Unit ={

    }


}
