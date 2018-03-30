package com.jiuzhong

import java.net.URL
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.util.Try


case class SadaRecord(srcip:String, ad:String, ts:String, url:String, ref:String, ua:String, dstip:String,cookie:String,
                      srcPort:String)
case class PVUrl(url:String)
case class ADUAURL(ad:String,ua:String,url:String)

object Estate {

  val conf = new SparkConf().setAppName("Oh!!")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.sql.crossJoin.enabled","true")
  conf.registerKryoClasses(Array(classOf[Enc],classOf[SadaRecord],classOf[PVUrl],classOf[ADUAURL]))
  val spark = SparkSession.builder().appName("OH!!").config(conf).getOrCreate()
  val sc = spark.sparkContext
  val todayStr = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
  val timeStr = new SimpleDateFormat("HHmmss").format(new java.util.Date())

  val fs = FileSystem.get(sc.hadoopConfiguration)

  import spark.implicits._


  def varsMap = Map(("delm","\t"),("underscore","_"),("ad","ad"),("total","_total"))

  private  def tagStringSuf(prjName:String)= prjName match {
    case "daoxila" => ":96928575"
    case "futures" => ":96928579"
    case _ => ""
  }


  private def createDir(path:String): Unit ={
    val pathDir = new Path(path).getParent

    if (!fs.exists(pathDir)){
      println(s"destination directory ${pathDir} not exists, creating ")
      fs.mkdirs(pathDir)
    }
  }
  private def deleteFile(fileName:String): Unit ={
    val path = fileName + "*"
    fs.listFiles(new Path(path),false)
    for (path <- fs.globStatus(new Path(path))){
      println(s"hadoop  file path: ${path.getPath} already exsits, deleting...")
      fs.delete(path.getPath,true)
    }
  }
  def scrapeSourcePV(configMap:Map[String,String],enc: Enc): Unit ={
    val sadaRecordArr = Array("srcip", "ad", "ts", "url", "ref", "ua", "dstip","cookie", "srcPort")
    val urlSchema = Encoders.product[PVUrl].schema
    val dataSchema = Encoders.product[SadaRecord].schema
    val formatName = "com.databricks.spark.csv"
    val decryptStr = udf{url:String =>
      enc.decrypt(url)
    }
    val encrypStr = udf{url:String=>
      enc.encrypt(url)
    }
    val getHost = udf{
      url:String =>
        Try{new URL(url).getHost}.getOrElse("")
    }
    val instrCol = udf{(str1:String,str2:String) =>
      str1.contains(str2)
    }
    val urls = spark.read.format(formatName).schema(urlSchema)
      .option("delimiter"," ").option("parserLib","univocity").load(configMap("urlPath"))
      .withColumn("url",decryptStr($"url"))
//    val datas  = configMap("sourcePath").split(",") map spark.read.format(formatName).option("parserLib","univocity").option("mode","DROPMALFORMED").schema(dataSchema).option("delimiter","\t").load
    val data = sc.textFile(configMap("sourcePath")).filter(line => {
      val arr = line.split("\t")
      arr.length>8 && arr(1) != "none" && arr(3).contains("sh.58.com/ershoufang")
      }).map{
      line =>
        val arr  = line.split("\t")
        (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))
    }.toDF(sadaRecordArr:_*).as[SadaRecord]
//    val data = datas.tail.foldLeft(datas.head){(df1,df2) => df1.unionAll(df2)}   .as[SadaRecord]

    val scrapeDS = data.as("t1").join(broadcast(urls).as("t2"),instrCol($"t1.url",$"t2.url"),"leftsemi")
      .withColumn("url",getHost($"url"))

    scrapeDS.write.format(formatName).option("delimiter","\t")
      .save(configMap("scrapePath"))
  }

  def scrapeScource(configMap:Map[String,String],enc:Enc): Unit = {
    //println(s"source files: ${adcookiePath} ${newclickPath} ${postPath}")
    val sadaRecordArr = Array("srcip", "ad", "ts", "url", "ref", "ua", "dstip","cookie", "srcPort")
      //    val decode64 = Base64.getDecoder
    val encryptString = udf { (url: String) =>
        enc.encrypt(url)
    }
    val instrUDF = udf{(col1:String,col2:String) =>
        col2.split(" ").forall(col1.contains(_))
    }
    val data = sc.textFile(configMap("sourcePath"))
    //    val data = sc.textFile("%s,%s,%s".format(adcookiePath,newclickPath,postPath))
    val urlsRDD = sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "")
    val saveTbl =
    if (urlsRDD.count() > 10000){
        val urlsDF = urlsRDD.toDF("url")
        data.map{
          line =>
            val arr:Array[String] = line.split("\t")
            (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))
          }.toDF(sadaRecordArr:_*).as("t1").join(urlsDF.as("t2"),instrUDF($"t1.url",$"t2.url"),"leftsemi")
          .as[SadaRecord].map{
          sada =>
            val hostname = Try{new URL(sada.url).getHost}.getOrElse("")
            val ref = if (sada.ref.toLowerCase == "nodef") "" else enc.encrypt(sada.ref)
            val ua = if ( sada.ua.toLowerCase  == "nodef") "" else sada.ua
            val cookie = if (sada.cookie.toLowerCase == "nodef") "" else sada.cookie
            (sada.srcip, sada.ad, sada.ts, hostname, ref, ua, sada.dstip, cookie, sada.srcPort)
          }
      } else{
        val urls = urlsRDD.collect()
        data.filter{
          arr =>
            val url = arr.split("\t")(3)
            urls.exists(l => l.split(" +").forall(url.contains(_)))
         }.map{
          line =>
            val arr:Array[String] = line.split("\t")
            val ref = if (arr(4).toLowerCase == "nodef") "" else enc.encrypt(arr(4))
            val ua = if ( arr(5).toLowerCase  == "nodef") "" else arr(5)
            val cookie = if (arr(7).toLowerCase == "nodef") "" else arr(7)
            val ad = arr(1)
          //        val url = enc.encrypt(arr(3).replaceAll("\\s+",""))
             val hostname = Try{new URL(arr(3)).getHost}.getOrElse("")
          //        val url = if (arr(3).toLowerCase() == "nodef") "" else  new URL(arr(3)).getHost
            (arr(0),ad,arr(2),hostname,ref,ua,arr(6),cookie,arr(8))
        }.toDF(sadaRecordArr:_*).as[SadaRecord]
      //      .withColumn("url", regexp_replace($"url","\t","")).
      //      withColumn("ref",regexp_replace(decode(unbase64($"ref"),"UTF-8"),"\t","")).
        //withColumn("ua",decode(unbase64($"ua"),"UTF-8")).
        //      withColumn("cookie",regexp_replace(decode(unbase64($"cookie"),"UTF-8"),"\\p{C}","?")).as[SadaRecord]
      }


//
//    val saveTbl = sourceDS.filter{
//      record =>
//        urls.exists(l => l.forall(record.url.contains(_)))
//    }.toDF(sadaRecordArr:_*)
    //println("src data count"+ dataFilter.count)
    saveTbl.coalesce(1000).write.format("com.databricks.spark.csv").
      option("delimiter", varsMap("delm"))
      .save(configMap("scrapePath"))
  }

  // filter data based ts col -- unxi epoch timestamp
  //case class Record(srcip:String, ad:String, ts:Long, url:String, ref:String, desip:String, cookie:String, src_port: String, tslag:Long);
  def processAcc(configMap:Map[String,String],enc: Enc): Unit ={
    // validinterval -- valid time interval in seconds
    // lowlimit -- min stay time in seconds
    // uplimit -- max stay time in seconds
    //println(s"filter data : source file ${srcpath}")
    //println(s"destination file: ${destpath}")
    //println(s"valid interval : ${validinterval}s, low limit ${lowlimit}, uplimit: ${uplimit}")
    val processDF = sc.textFile(configMap("scrapePath")).map{
        l =>
         val arr = l.split("\t")
        (arr(1),arr(5),arr(3))
    }.distinct().toDF("ad","ua","url")
    processDF.coalesce(10).write.format("com.databricks.spark.csv").option("delimiter","\t").save(configMap("processPath"))
  }

  def processPv(configMap: Map[String, String], enc: Enc): Unit ={
    val interval = configMap.getOrElse("interval","300").toInt
    val lowLimit = configMap.getOrElse("lowLimit","300").toInt
    val upLimit = configMap.getOrElse("upLimit","100800").toInt

    val sadaRecordArr = Array("srcip", "ad", "ts", "url", "ref", "ua", "dstip","cookie", "srcPort")
    val delm = varsMap("delm")
    val srcData = sc.textFile(configMap("scrapePath")).map{
      row =>
        val arr = row.split(delm)
        (arr(0), arr(1),arr(2).toLong,Try{new URL(arr(3)).getHost}.getOrElse(""), arr(4), arr(5), arr(6), arr(7), arr(8))
    }.toDF(sadaRecordArr:_*).as[SadaRecord]

    val wSpec = Window.partitionBy("ad","ua").orderBy($"ts")
    val dataSort = srcData.withColumn("tslag", lag("ts", 1, 0).over(wSpec)).filter($"tslag" =!= 0)

    val dataValid = dataSort.withColumn("interval", ($"ts" - $"tslag")/1000).filter($"interval" < interval).
      groupBy("ad","ua","url").agg(sum($"interval") as "totaltime", max("srcip") as "srcip", max("ref") as "ref",
      max("dstip") as "dstip", max("cookie") as "cookie", max("srcPort") as "srcPort")

    val dataFilter = dataValid.filter($"totaltime" >= lowLimit && $"totaltime" <= upLimit).select(
      "srcip", "ad", "totaltime", "url", "ref", "ua", "dstip","cookie", "srcPort")
    val dataFilterMatch = dataFilter.select("ad","ua","url")//.filter("ad != '' and ad != 'none'")

    //  add count

    val countValid = srcData.groupBy("ad","ua").agg(count("url") as "cnts", max("url") as "url").filter($"cnts" between(1,100))
      .drop("cnts")

    dataFilterMatch.union(countValid).dropDuplicates("ad","ua").coalesce(10)
        .write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("processPath"))
//    dataFilter.write.format("com.databricks.spark.csv").option("delimiter","\t").save(destpath)
//    println("after filter, data count: " + dataFilter.count)

  }

  //case class Record(mobile:String, url:String)
  def matchPortal( configMap:Map[String,String]): String ={
    val matchMode = configMap("matchMode")
    println(s"match mode: ${matchMode}")
    val pieceAmount = configMap.getOrElse("pieceAmount","10000").toInt
    val delm = varsMap("delm")
    val data = sc.textFile(configMap("processPath")).map{
      row =>
        val arr =row.split(delm)
        (arr(0),arr(1),arr(2))
    }

    val counts:Int = math.ceil(data.count()/pieceAmount.toFloat).toInt
    import hlwbbigdata.phone
    val matchPortalPath = configMap("matchPortalPath")
    if (counts > 1){
      val dataPieces =data.randomSplit(Array.fill(counts)(1))
      for  ((piece,idx) <- dataPieces.zipWithIndex) {
        val matchResult = phone.phone_match(spark, piece, matchMode)
        matchResult.write.format("com.databricks.spark.csv").option("delimiter", delm).save( matchPortalPath + "_" + idx)
      }
      matchPortalPath
    }else{
      val matchResult = phone.phone_match(spark, data, matchMode)
      matchResult.write.format("com.databricks.spark.csv").option("delimiter", delm).save( matchPortalPath)
      ""
    }
  }

  def combineMatchPortal(filePath:String): Unit ={
    val res = sc.textFile(filePath +"_*")
    res.saveAsTextFile(filePath)
  }



  def dropHistory(prjName:String,tagName:String, configMap:Map[String,String]): Unit ={
    val delm = varsMap("delm")
    val tagNames =sc.textFile(configMap("tagPath")).map(l => l.split(" +")).collect()
    val inDF = sc.textFile(configMap("matchPortalPath")).map(row => (row.split(delm)(0), row.split(delm)(1))).map{
      row =>
        val appName = tagNames.filter(l => row._2.contains(l(0)) || l(0) == "*").
          map(l => l(1)).headOption.getOrElse("")
        (row._1, appName)
    }.toDF("mobile","url").filter("url != ''").dropDuplicates()

    val tagString = ":" + tagName + "_" + todayStr  + tagStringSuf(prjName)


    val hisDF = sc.textFile(configMap("historyPath")).map(row => row.split(delm)(0)).toDF("mobile")
    val tagDF = inDF.join(hisDF,Seq("mobile"),"leftanti").withColumn("tag", concat($"url",lit(tagString))).drop($"url").dropDuplicates(Seq("mobile"))
    tagDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("dropHistoryPath"))

  }

  def kvTag( key:String,configMap:Map[String,String]): Unit ={
    val delm = varsMap("delm")
    val underScore = varsMap("underscore")
    val ad  = varsMap("ad")
    val total = varsMap("total")
    //    val inDF = sc.textFile(inPath).map{
    //      row =>
    //        val arr = row.split(delm,2)
    //        (arr(0),arr(1))
    //    }.zipWithIndex().map(
    //      row =>
    //        (key + underScore + todayStr + underScore + row._2, ad + delm + row._1._2 + delm + row._1._1)
    //    ).toDF("key","value")

    val inDF = sc.textFile(configMap("dropHistoryPath")).map{
      row =>
        val arr = row.split(delm,2)
        (arr(0),arr(1))
    }.toDF("key","value").
      select(lit(key + underScore + todayStr + underScore), row_number().over(Window.orderBy("value") )
        ,concat(lit(ad + delm),$"value" ,lit(delm), $"key")).toDF("k1","k2","value")
      .withColumn("key",concat( $"k1",$"k2" - 1))
      .select("key","value")

    val totalLine = (key + underScore + todayStr+ total, inDF.count.toString )
    val counts = Seq(totalLine).toDF("key","value")

    val kvTbl = inDF.union(counts)
    val historyDF = inDF.select("value").map(row => row.getString(0).split(delm)(2))
    kvTbl.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("kvPath"))
    historyDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("saveHistoryPath"))

  }

  def getConfig(prjName:String, method:String,dateStr:String,mode:String = "none"):Map[String,String]={
    // 得到每个项目不同方式下的配置文件: source file, url file, tag file
    val user = System.getProperty("user.name")

    val publicPath = "hdfs://ns1/user/gdpi/public"
    val addcookiePath = s"${publicPath}/sada_gdpi_adcookie/${dateStr}/*/*.gz"
    val newclickPath = s"${publicPath}/sada_new_click/${dateStr}/*/*.gz"
    //    val postPath = ""
    val postPath = s"${publicPath}/sada_gdpi_post_click/${dateStr}/*/*.gz"

    val sourcePath = if (user == "u_tel_hlwb_mqj") "%s,%s".format(addcookiePath, newclickPath)
    else "%s,%s,%s".format(addcookiePath,newclickPath,postPath)

    val dateStrModified = dateStr.replace("*","_").replace("{","").replace("}","").replace(",","_")
    val privateBasePath = s"hdfs://ns1/user/${user}/private/test"

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

    // default configuration return
    val baseCFG = Map(
      "sourcePath" -> sourcePath,
      "urlPath" -> urlPath,
      "tagPath" -> tagPath,
      "scrapePath" -> scrapePath,
      "processPath" -> processPath,
      "historyPath" -> historyPath
    )
    val configBasePath = s"${privateBasePath}/config"
    val allCfgPath = s"${configBasePath}/all.cfg"
    val cfgPath = s"${configBasePath}/${prjName}_${method}.cfg"
    val cfg =
    if (fs.exists(new Path(cfgPath))) {
      val cfg = sc.textFile(cfgPath).filter(!_.startsWith("#")).map {
        l =>
          var arr = l.split(" +")
          arr(0) -> arr(1)
      }.collect().toMap[String,String]
      baseCFG ++ cfg
    }else if( fs.exists(new Path(allCfgPath)) && sc.textFile(allCfgPath).filter(_.contains(s"${prjName}_${method}")).count() == 1) {
      val cfgPath = sc.textFile(allCfgPath).filter(!_.startsWith("#"))
        .filter(   _.contains(s"${prjName}_${method}")).take(1)(0).split(" +")(1)

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
    cfg.foreach(pair => println(pair._1+" : "+pair._2))

    val matchMode = if (mode != "none") mode else cfg.getOrElse("matchMode","1")

    val saveMatchPath = Map(
      "matchMode" -> matchMode,
      "matchPortalPath" -> "%s_%s".format(matchPortalPath , matchMode),
      "dropHistoryPath" -> "%s_%s".format(dropHistoryPath,matchMode),
      "kvPath" -> "%s_%s".format(kvPath,matchMode),
      "saveHistoryPath" -> "%s_%s".format(saveHistoryPath,matchMode)
    )

    cfg ++ saveMatchPath

//    // history file path
//    val historyPath = s"${privateBasePath}/${prjName}_final_history/*"
//    val saveHistoryPath = s"${privateBasePath}/${prjName}_final_history/${prjName}_${dateStr}"
  }

  def change_tag(filePath:String, newKey:String, newTag:String, newSubKey:String):Unit ={
//    println("in change_tag ...")
//    println(s"newkey: ${newKey}, newTag: ${newTag}, newSubKey: ${newSubKey}")
    val kv = sc.textFile(filePath).map {
      line =>
        val arr = line.split("\t",2)
        arr(0) = if (newKey == "") arr(0) else newKey + "_" + arr(0).split("_").last
        arr(1) =
          if (newTag == "" && newSubKey == "") {
            arr(1)
          }
          else {
            val valueArr = arr(1).split("\t")
            // total status line remain unchange
            if (valueArr.size == 1) arr(1)
            else {
              val subValueArr = valueArr(1).split(":")
              subValueArr(0) = if (newTag == "") subValueArr(0) else newTag
              subValueArr(1) = if (newSubKey == "") subValueArr(1) else newSubKey
              valueArr(1) = subValueArr.mkString(":")
              valueArr.mkString("\t")
            }
          }
       arr.mkString("\t")
    }
//    deleteFile(filePath)
    kv.saveAsTextFile(filePath+s"_ChangeTag_${timeStr}")
  }
  def run_all_acc(prjName:String, method:String, dateStr:String,tagName:String,enc: Enc): Unit ={
    val cfgs = getConfig(prjName,method,dateStr)
    scrapeScource(cfgs,enc)
    processAcc(cfgs,enc)

    val path = matchPortal(cfgs)
    if (path != "")
      combineMatchPortal(path)


    dropHistory(prjName,tagName,cfgs)
    kvTag(tagName,cfgs)
  }

  def run_all_pv(prjName: String, method: String, dateStr: String, tagName: String, enc: Enc): Unit ={
    val cfgs = getConfig(prjName, method, dateStr)
    scrapeScource(cfgs, enc)
    processPv(cfgs,enc)

    val path = matchPortal(cfgs)
    if (path != "")
      combineMatchPortal(path)

    dropHistory(prjName,tagName,cfgs)
    kvTag(tagName,cfgs)
  }

  def run_scape_from_pv(prjName: String, method: String, dateStr: String, tagName: String, enc: Enc): Unit ={
    val cfgs  = getConfig(prjName,method,dateStr)
    scrapeSourcePV(cfgs,enc)
    processPv(cfgs,enc)

    val path = matchPortal(cfgs)
    if (path != "")
      combineMatchPortal(path)

    dropHistory(prjName,tagName,cfgs)
    kvTag(tagName,cfgs)
  }
  def run_process_acc(prjName:String,method:String,dateStr:String, tagName:String,enc: Enc): Unit ={
    val cfgs = getConfig(prjName,method,dateStr)
    deleteFile(cfgs("processPath"))
    deleteFile(cfgs("matchPortalPath"))
    deleteFile(cfgs("dropHistoryPath"))
    deleteFile(cfgs("kvPath"))
    deleteFile(cfgs("saveHistoryPath"))

    processAcc(cfgs,enc)

    val path = matchPortal(cfgs)
    if (path != "")
      combineMatchPortal(path)

    dropHistory(prjName,tagName,cfgs)
    kvTag(tagName,cfgs)
  }
  def run_process_pv(prjName:String,method:String,dateStr:String, tagName:String,enc: Enc): Unit ={
    val cfgs = getConfig(prjName,method,dateStr)
    deleteFile(cfgs("processPath"))
    deleteFile(cfgs("matchPortalPath"))
    deleteFile(cfgs("dropHistoryPath"))
    deleteFile(cfgs("kvPath"))
    deleteFile(cfgs("saveHistoryPath"))

    processPv(cfgs,enc)

    val path = matchPortal(cfgs)
    if (path != "")
      combineMatchPortal(path)

    dropHistory(prjName,tagName,cfgs)
    kvTag(tagName,cfgs)
  }

  // arg 0 : project name must supply.
  def main(args: Array[String]): Unit = {
    assert(args.length >= 1, "stage name must supply")
    val cmd = args(0)
    val prjName = args.lift(1).getOrElse("")
    val method = args.lift(2).getOrElse("")
    val dateStr = if (args.length < 2) new SimpleDateFormat("yyyyMMdd").format(new java.util.Date()) else args.lift(3).getOrElse("")

    val enc = new Enc("a123bcd#$e45!@%","jnM34G6NHkqMhKlOuJo9VhLAqOpF0BePojHgh1010GHgNg8^72k")

    (cmd,prjName,method,dateStr) match {
      case ("test_conf",prj,mth,ds) => getConfig(prj,mth,ds)
      case ("change_tag",arg1,arg2,arg3) => change_tag(arg1,arg2,arg3,args.lift(4).getOrElse(""))
      case("run_all",arg1,"acc",arg3) => run_all_acc(arg1,"acc",arg3,args.lift(4).getOrElse("") ,enc)
      case("run_all",arg1,"pv",arg3) => run_all_pv(arg1,"pv",arg3, args.lift(4).getOrElse(""), enc)
      case("run_scrape_from_pv",arg1,"pv",arg3) => run_scape_from_pv(arg1,"pv",arg3,args.lift(4).getOrElse(""),enc)
      case("run_process",arg1,"pv",arg3) => run_process_pv(arg1,"pv",arg3,args.lift(4).getOrElse(""),enc)
      case("run_process",arg1,"acc",arg3) =>run_process_acc(arg1,"acc",arg3,args.lift(4).getOrElse(""),enc)
    }
  }

}
