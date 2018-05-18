package com.jiuzhong.utils

import java.text.SimpleDateFormat

import com.jiuzhong.Estate.fs
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.regexp_replace

object utils extends java.io.Serializable{
    val  todayStr: String = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
    val timeStr: String = new SimpleDateFormat("HHmmss").format(new java.util.Date())

    def deleteGolbalFile(fileName:String): Unit ={
        val path = fileName + "*"
        for (path <- fs.globStatus(new Path(path))){
            println(s"hadoop  file path: ${path.getPath} already exsits, deleting...")
            fs.delete(path.getPath,true)
        }
    }

    def  writeData[T](DF:Dataset[T],path:String, num:Int=1000): Unit ={
        DF.coalesce(num).write.format("csv").option("delimiter","\t").option("ignoreTrailingSpace",false).save(path)
    }

    def readData(spark:SparkSession,path:String): DataFrame = {
        spark.read.option("delimiter","\t").csv(path)
    }

    def matchPortal(spark:SparkSession, configMap:Map[String,String]):Unit= {
        import spark.implicits._
        val matchMode = configMap("matchMode")

        println(s"match mode: ${matchMode}")

        val delm = "\t"
        val data = spark.read.format("csv").option("delimiter",delm).load(configMap("processPath")).toDF("ad","ua","url")
            .withColumn("ad",regexp_replace($"ad","^AD_","")).as[ADUAURL]
        data.createTempView("data")

        val phonePath = "/user/g_tel_hlwb_data/public/hlwb_data1/device_info/"
        val filterPhonePath = "/user/g_tel_hlwb_data/public/hlwb_data1/phone_set/"
        //        val adPath = "private/test/config/ad.txt"
        val phone = spark.read.parquet(phonePath)
        //        val ad  = spark.read.format("csv").option("delimiter","\t").load(adPath).toDF("ad_phone","ad_data")
        // 添加phone和data ad的关联表
        //        phone.createTempView("basePhone")
        val filterPhone = spark.read.textFile(filterPhonePath)
        phone.createTempView("phone")
        filterPhone.createTempView("filter_phone")
        //        ad.createTempView("ad")

        //        spark.sql(
        //            """
        //              |select ad.ad_data as ad, basePhone.ua, basePhone.phone
        //              |     , basePhone.weight, basePhone.date, basePhone.rtype
        //              |from basePhone inner join ad on basePhone.ad = ad.ad_phone
        //              |
        //            """.stripMargin).createTempView("phone")

        val accuracyResult = spark.sql(
            """
              |select phone, url, ad
              |from
              |	(select phone, url, ad, row_number() over(partition by ad, ua order by date desc ,weight desc) as rank
              |	from
              |		(select phone.ad , phone.ua , date, weight, phone.phone, url
              |		from phone join data on phone.ad = data.ad and phone.ua = data.ua
              |		) tmp
              |	) tmp
              |where rank = 1
            """.stripMargin)
        //        accuracyResult.show(false)
        accuracyResult.createTempView("accuracy_result")
        val fuzzyResult = spark.sql(
            """
              |select phone, concat(url,"_fuz") as url
              |from
              |	(select ad, phone, row_number() over(partition by ad order by type, date desc ,weight desc) as rank
              |	from
              |		(select phone.ad, if(ua like '%Windows%' or phone.ua like '%Mac OS%', 1, 0) as type, date, weight, phone.phone
              |		from phone
              |			left outer join accuracy_result on phone.ad = accuracy_result.ad
              |		where accuracy_result.phone is null
              |		)
              |	) phone
              |		join (select data.ad, data.url from data left outer join accuracy_result on data.ad = accuracy_result.ad where accuracy_result.phone is null) data on phone.ad = data.ad
              |where rank = 1
              |union
              |select phone, concat(url,"_acc") as url from accuracy_result
            """.stripMargin)
        //        fuzzyResult.show(false)
        fuzzyResult.createTempView("fuzzy_result")
        val result = spark.sql(
            """
              |select fuzzy_result.phone , fuzzy_result.url
              |from fuzzy_result
              |where exists (
              |     select 1
              |     from filter_phone
              |     where filter_phone.value = fuzzy_result.phone
              |)
            """.stripMargin)
        //        result.show(false)
        writeData(result,configMap("matchPortalPath"),100)
        //      val counts:Int = math.ceil(data.count()/pieceAmount.toFloat).toInt
        //        val matchPortalPath = configMap("matchPortalPath")
        //      if (counts > 1){
        //        val dataPieces =data.randomSplit(Array.fill(counts)(1))
        //        for  ((piece,idx) <- dataPieces.zipWithIndex) {
        //          val matchResult = phone.phone_match(spark, piece, matchMode)
        //          matchResult.write.format("com.databricks.spark.csv").option("delimiter", delm).save( matchPortalPath + "_" + idx)
        //        }
        //         matchPortalPath
        //      }else{
        //        val matchResult = phone.phone_match(spark, data, matchMode)
        //        matchResult.write.format("com.databricks.spark.csv").option("delimiter", delm).save( matchPortalPath)
        //        ""
        //      }

    }


}
