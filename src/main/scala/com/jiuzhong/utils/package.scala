package com.jiuzhong

import com.jiuzhong.utils.utils.todayStr
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, SparkSession}

package object utils {
    case class SadaRecord(srcip:String, ad:String, ts:String, url:String, ref:String, ua:String, dstip:String,cookie:String,
                          srcPort:String,tbd:String)
    case class PVUrl(url:String)
    case class ADUAURL(ad:String,ua:String,url:String)
    case class URLRecord(host:String,url:String,ref:String,cookie:String)

    def getConfig(spark:SparkSession, prjName:String, method:String,dateStr:String,mode:String = "none"):Map[String,String]={
        // 得到每个项目不同方式下的配置文件: source file, url file, tag file
        val sc = spark.sparkContext
        val fs = FileSystem.get(sc.hadoopConfiguration)

        val user = System.getProperty("user.name")

        val publicPath = "/user/db_gdpi/public"
        //        val addcookiePath = s"${publicPath}/sada_gdpi_adcookie/${dateStr}/*/*.gz"
        //        val newclickPath = s"${publicPath}/sada_gdpi_click/${dateStr}/*/*.gz"
        //        //    val postPath = ""
        //        val postPath = s"${publicPath}/sada_gdpi_post_click/${dateStr}/*/*.gz"
        val adcookiePath = "db_gdpi.sada_gdpi_click"
        val newclickPath = "db_gdpi.sada_gdpi_adcookie"
        //        val sourcePath = if (user == "u_tel_hlwb_mqj") "%s,%s".format(addcookiePath, newclickPath)
        //            else "%s,%s,%s".format(addcookiePath,newclickPath,postPath)
        val sourcePath = "%s,%s".format(adcookiePath,newclickPath)
        val dateStrModified = dateStr.replace("*","_").replace("{","").replace("}","").replace(",","_").replace("[","_").replace("]","_")
        val privateBasePath = s"/user/${user}/private/test"

        //default configuration
        //url and tag file
        val urlPath = s"${privateBasePath}/config/${prjName}_${method}_url.txt"
        val tagPath = s"${privateBasePath}/config/${prjName}_${method}_tag.txt"
        // private path
        val privatePath = s"${privateBasePath}/${prjName}/${dateStrModified}/${method}"
        val scrapePath = s"${privatePath}/scrape"
        val processPath = s"${privatePath}/process"
        val matchPortalPath = s"${privatePath}/match_portal"
        val dropHistoryPath = s"${privatePath}/drop_history"
        val kvPath = s"${privatePath}/kv"
        // history path
        val historyPath = s"${privateBasePath}/${prjName}_final_history/*"
        val saveHistoryPath = s"${privateBasePath}/${prjName}_final_history/${method}_${dateStrModified}"
        val clientID = prjName

        // default configuration return
        val baseCFG = Map(
            "sourcePath" -> sourcePath,
            "urlPath" -> urlPath,
            "tagPath" -> tagPath,
            "scrapePath" -> scrapePath,
            "processPath" -> processPath,
            "historyPath" -> historyPath,
            "clientID" -> clientID,
            "todayStr" -> todayStr
        )
        val configBasePath = s"${privateBasePath}/config"
        val allCfgPath = s"${configBasePath}/all.cfg"
        val cfgPath = s"${configBasePath}/${prjName}_${method}.cfg"
        val cfg = if (fs.exists(new Path(cfgPath))) {
            val cfg = sc.textFile(cfgPath).filter(!_.startsWith("#")).filter(_.trim() != "").map {
                l =>
                    var arr = l.split(" +")
                    arr(0) -> arr(1)
            }.collect().toMap[String,String]
            baseCFG ++ cfg
        }else if( fs.exists(new Path(allCfgPath)) && sc.textFile(allCfgPath).filter(_.contains(s"${prjName}_$method")).count() == 1) {
            val cfgPath = sc.textFile(allCfgPath).filter(!_.startsWith("#"))
                .filter(   _.contains(s"${prjName}_$method")).take(1)(0).split(" +")(1)

            val cfg = sc.textFile(configBasePath+"/"+ cfgPath).map{
                l =>
                    val arr = l.split(" +")
                    arr(0) -> arr(1)
            }.collect().toMap
            baseCFG ++ cfg
        }else{
            //      val cfg = Map[String,String]()
            //      throw new SparkException("config file for %s %s not found or define more than once in all.cfg".format(prjName,method))
            println("no config file find use default settings")
            baseCFG
        }

        val matchMode = if (mode != "none") mode else cfg.getOrElse("matchMode","1")
        val saveMatchPath = Map(
            "matchMode" -> matchMode,
            "matchPortalPath" -> "%s_%s".format(matchPortalPath , matchMode),
            "dropHistoryPath" -> "%s_%s".format(dropHistoryPath,matchMode),
            "kvPath" -> "%s_%s".format(kvPath,matchMode),
            "saveHistoryPath" -> "%s_%s".format(saveHistoryPath,matchMode)
        )
        cfg.foreach(pair => println(pair._1+" : "+pair._2))

        cfg ++ saveMatchPath

        // history file path
        //   val historyPath = s"${privateBasePath}/${prjName}_final_history/*"
        //    val saveHistoryPath = s"${privateBasePath}/${prjName}_final_history/${prjName}_${dateStr}"
    }

    implicit class ColumnAdd(c:Column){
        val  mkStringUDF: UserDefinedFunction = udf{(arr:Seq[String],sep:String) =>
                arr.mkString(sep)
        }
        def mkString(s:String):Column ={
            mkStringUDF(c,lit(s))
        }
    }

}
