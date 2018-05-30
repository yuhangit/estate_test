package com.jiuzhong

import java.net.URL
import java.text.SimpleDateFormat

import com.jiuzhong.scraper.{ScrapeCDPI, ScrapeLTE}
import com.jiuzhong.utils.getConfig
import com.jiuzhong.utils.{ADUAURL, PVUrl, SadaRecord, URLRecord}
import com.jiuzhong.utils.utils.writeData
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.util.Try



object Estate {

    val conf = new SparkConf().setAppName("Oh!!")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.crossJoin.enabled","true")
    conf.set("spark.speculation","true")
    conf.registerKryoClasses(Array(classOf[Enc],classOf[SadaRecord],classOf[PVUrl],classOf[ADUAURL]))
    val spark = SparkSession.builder().enableHiveSupport().appName("OH!!").config(conf).getOrCreate()
    val sc = spark.sparkContext
    val  todayStr: String = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
    val timeStr: String = new SimpleDateFormat("HHmmss").format(new java.util.Date())

    val fs = FileSystem.get(sc.hadoopConfiguration)

    import spark.implicits._

    val sadaRecordArr = Array("srcip", "ad", "ts", "url", "ref", "ua", "dstip", "cookie", "srcPort","tbd","datelabel","timestamp","source")
    def varsMap = Map(("delm","\t"),("underscore","_"),("ad","ad"),("total","_total"))

    // str1 is outer table filed and str2 is inner table field -- run as the beginning of the job
    private val inStrUDF = udf{(str1:String,str2:String) =>
        if (str2 == null ) true else if(str1 == null) false else str2.split(" +").forall(str1.contains(_))
    }
    spark.udf.register("inString",inStrUDF)

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
    private def deleteGolbalFile(fileName:String): Unit ={
        val path = fileName + "*"
        for (path <- fs.globStatus(new Path(path))){
            println(s"hadoop  file path: ${path.getPath} already exsits, deleting...")
            fs.delete(path.getPath,true)
        }
      }

    def scrapeCDPI(configMap: Map[String, String], dateStr: String, enc: Enc): Unit = {
        val urlsRDD= sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "")
        urlsRDD.saveAsTextFile(configMap("urlPath")+"asda")
        val urls = spark.read.format("json").load(configMap("urlPath") +"asda").as[URLRecord].cache()
        urls.createTempView("urls")
        urls.count()
        deleteGolbalFile(configMap("urlPath") +"asda")
        val hosts = urls.map(_.host).distinct().collect()
        val stmt =s"""
          |select * from db_cdpi.sada_lte_userurl where dt rlike '${dateStr}'
          |union all
          |select *
        """.stripMargin


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
    @deprecated
    def scrapeLTE(configMap: Map[String, String], dateStr :String,enc: Enc): Unit = {

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
    @deprecated
    def scrapeSourcePV(configMap:Map[String,String],enc: Enc): Unit ={
        val urlSchema = Encoders.product[PVUrl].schema
        val dataSchema = Encoders.product[SadaRecord].schema
        val formatName = "com.databricks.spark.csv"
        val decryptStr = udf{url:String =>
            enc.decrypt(url)
        }
        val encrypStr = udf{url:String=>
            enc.encrypt(url)
        }
        val getHost = udf{url:String =>
            Try{new URL(url).getHost}.getOrElse(url)
        }
        val instrCol = udf{(str1:String,str2:String) =>
            str2.split(" +").forall(str1.contains(_))
        }
    //  each line will not split to many parts becasue of encrypt and base64 encode
        val urls = spark.read.format(formatName).schema(urlSchema)
            .option("delimiter"," ").option("parserLib","univocity").load(configMap("urlPath"))
            .withColumn("url",decryptStr($"url")).as[PVUrl]
        urls.createOrReplaceTempView("t2")

        val hosts = urls.map{
            line =>
                val url = line.url.split(" +")(0)
                // filter host nor url itself.
                Try{new URL(url).getHost}.getOrElse(url)
        }.filter(_ != "").distinct().collect()
        //    val datas  = configMap("sourcePath").split(",") map spark.read.format(formatName).option("parserLib","univocity").option("mode","DROPMALFORMED").schema(dataSchema).option("delimiter","\t").load
        val data  = sc.textFile(configMap("sourcePath")).filter(line => {
            val arr = line.split("\t")
            // filter map.baidu.com or share.baidu.com
            arr.length>8 && arr(1) != "none" && hosts.exists(l => l.split(" +").forall(arr(3).contains(_)))
        }).map{
            line =>
                val arr  = line.split("\t")
                val ts = arr(2).dropRight(3)
                (arr(0),arr(1),ts,arr(3),arr(4),arr(5),arr(6),arr(7),arr(8))
        }.toDF(sadaRecordArr:_*)
            .as[SadaRecord]

        data.createTempView("t1")
        //    val data = datas.tail.foldLeft(datas.head){(df1,df2) => df1.unionAll(df2)}   .as[SadaRecord]

        val scrapeDS = data.as("t1").join(urls.as("t2"),instrCol($"t1.url",$"t2.url"),"leftsemi")
        //      .withColumn("url",getHost($"url"))
        //    val scrapeDS = spark.sql("select t1.* from t1 left semi join t2 on instr(t1.url,t2.url)> 0")
        //          .withColumn("url",getHost($"url"))
        scrapeDS.coalesce(10000).write.format(formatName).option("delimiter","\t")
            .save(configMap("scrapePath"))
    }


    def scrapeSourceAcc(configMap:Map[String,String],dateStr:String ,enc:Enc): Unit = {
        //println(s"source files: ${adcookiePath} ${newclickPath} ${postPath}")
      //    val decode64 = Base64.getDecoder

        val urlsRDD= sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "")
        urlsRDD.saveAsTextFile(configMap("urlPath")+"asda")
        val urls = spark.read.format("json").load(configMap("urlPath") +"asda").as[URLRecord].cache()
        urls.createTempView("urls")
        urls.count()
        deleteGolbalFile(configMap("urlPath") +"asda")
        val hosts = urls.map(_.host).distinct().collect()


//        val sourceArr = configMap("sourcePath").split(",").map( path =>
//            spark.read.format("csv").option("delimiter","\t").option("parserLib","univocity")
//                .load(path)
//                .toDF(sadaRecordArr:_*)
//                .withColumn("ts",from_unixtime(substring($"ts",0,10)))
//                .as[SadaRecord].withColumn("tbd",lit(path.split("/").reverse.apply(3)))
//        )

        val sourceArr = configMap("sourcePath").split(",").map { path =>
            val sourceName = path.split("\\.")(1)
            val stmt = s"select t.*, '${sourceName}' as source from ${path} t where datelabel rlike '${dateStr}' "
            spark.sql(stmt)
        }

        val srcData = sourceArr.dropRight(1).foldRight(sourceArr.last){
            (arr,b) =>
                b.union(arr)
        }

        val filterCol = udf{(url:String) =>
            if (url == null) false
            else
            hosts.exists(url.contains(_))
        }
        //
        //        val saveTbl = srcData.as("t1").join(urls.as("t2"),instringCol($"t1.url",$"t1.ref",$"t2.cookie", $"t2.url",$"t2.ref",$"t2.cookie"), "leftsemi")
        srcData.where(filterCol($"url"))
                .createTempView("source_data")

        val data  = spark.sql("""select t1.* from source_data t1
                             where exists
                                (select 1 from urls t2
                                where inString(t1.url,t2.url)
                                    and inString(t1.cookie, t2.cookie)
                                    and inString(t1.ref,t2.ref)
                                )""")

        writeData(data,configMap("scrapePath"),10000)
//        data.coalesce(10000).write.format("com.databricks.spark.csv").
//            option("delimiter", varsMap("delm")).
//            save(configMap("scrapePath"))
    }
    @deprecated
    def scrapeScourceAcc_(configMap:Map[String,String], enc:Enc): Unit = {
        //println(s"source files: ${adcookiePath} ${newclickPath} ${postPath}")
        //    val decode64 = Base64.getDecoder
        val encryptString = udf{ (url: String) =>
            enc.encrypt(url)
        }
        val data = sc.textFile(configMap("sourcePath"))
        //    val data = sc.textFile("%s,%s,%s".format(adcookiePath,newclickPath,postPath))
        val urlsRDD = sc.textFile(configMap("urlPath")).map(l => enc.decrypt(l)).filter(_.trim != "").collect()
        val saveTable = data.filter{
            l =>
                val arr = l.split("\t")
                val url = arr(3)
                val ref:String = new String(Base64.decodeBase64(arr(4)),"utf-8")
                arr.length==10 && arr(1) != "none" && urlsRDD.exists(l => l.split(" +").forall(ele => url.contains(ele) || ref.contains(ele)))
        }.map{
            line =>
                val arr:Array[String] = line.split("\t")
                val ref = if (arr(4).toLowerCase == "nodef") "" else arr(4)
                val ua = if ( arr(5).toLowerCase  == "nodef") "" else arr(5)
                val cookie = if (arr(7).toLowerCase == "nodef") "" else arr(7)
                val ad = arr(1)
                val ts = arr(2).dropRight(3)
                (arr(0),ad,ts,arr(3),ref,ua,arr(6),cookie,arr(8))
        }.toDF(sadaRecordArr:_*)
            .withColumn("ref",regexp_replace(decode(unbase64($"ref"),"UTF-8"),"\t",""))
            .withColumn("ts",from_unixtime($"ts"))

        saveTable.coalesce(10000).write.format("com.databricks.spark.csv").
            option("delimiter", varsMap("delm")).
            save(configMap("scrapePath"))
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
        // !!! can't use native split method because of \t in column
        val processDF =spark.read.format("com.databricks.spark.csv").option("delimiter","\t").load(configMap("scrapePath")).
            toDF(sadaRecordArr:_*).select("ad","ua","url")
        processDF.coalesce(100).write.format("com.databricks.spark.csv").option("delimiter","\t").save(configMap("processPath"))

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
                (arr(0), arr(1),arr(2).toLong,Try{new URL(arr(3)).getHost}.getOrElse(arr(3)), arr(4), arr(5), arr(6), arr(7), arr(8))
        }.toDF(sadaRecordArr:_*).as[SadaRecord]

        val wSpec = Window.partitionBy("ad","ua").orderBy($"ts")
        val dataSort = srcData.withColumn("tslag", lag("ts", 1, 0).over(wSpec)).filter($"tslag" =!= 0)

        val dataValid = dataSort.withColumn("interval", ($"ts" - $"tslag")/1000).filter($"interval" < interval).
            groupBy("ad","ua","url").agg(sum($"interval") as "totaltime", max("srcip") as "srcip", max("ref") as "ref",
            max("dstip") as "dstip", max("cookie") as "cookie", max("srcPort") as "srcPort")

        val dataFilter = dataValid.groupBy("ad","ua").agg(sum("totaltime") as "totaltime", max("url") as "url")
            .filter($"totaltime" >= lowLimit && $"totaltime" <= upLimit)
            .select("ad","ua","url")//.filter("ad != '' and ad != 'none'")

        //  add count
        val countValid = srcData.groupBy("ad","ua").agg(count("url") as "cnts", max("url") as "url").filter($"cnts" between(10,500))
            .select("ad","ua","url")

        countValid.union(dataFilter).coalesce(10)
          .write.format("com.databricks.spark.csv").option("delimiter","\t").save(configMap("processPath"))

    }

  //case class Record(mobile:String, url:String)
    def matchPortal( configMap:Map[String,String]):Unit= {
        val matchMode = configMap("matchMode")
        println(s"match mode: ${matchMode}")

        val delm = varsMap("delm")
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
//        tagDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("dropHistoryPath"))
        writeData(tagDF,configMap("dropHistoryPath"),1)

    }

    def kvTag( key:String,configMap:Map[String,String]): Unit ={
        val clientID = configMap("clientID")
        val requirementID = key.split("_")(0)
        val serialID = key.split("_")(1)
        val inDF = spark.read.format("csv").option("delimiter","\t")
            .load(configMap("dropHistoryPath")).toDF("mobile","tag")

        inDF.createTempView("inDF")
        val kv = spark.sql(
            s"""
              |select concat(concat_ws('_','$clientID','$requirementID','$todayStr','$serialID',
              |                     row_number() over (order by tag) -1),'\t') as key,
              |       concat_ws('\t',tag,mobile) as value
              |
              |from inDF
            """.stripMargin)
//        select(lit(key + underScore + todayStr + underScore), row_number().over(Window.orderBy("tag") )
//            ,concat(lit(ad + delm),$"tag" ,lit(delm), $"mobile")).toDF("k1","k2","value")
//            .withColumn("",concat( $"k1",$"k2" - 1))
//            .select("key","value")

        kv.createTempView("kv")
        val kvTbl = spark.sql(
            s"""
              |select key,value from kv
              |union
              |select concat_ws('_','$clientID','$requirementID','$todayStr','$serialID','total') , count(*)
              |from kv
            """.stripMargin)
        val historyDF = inDF.select("mobile")
        writeData(historyDF,configMap("saveHistoryPath"),1)
        writeData(kvTbl,configMap("kvPath"),1)
//        kvTbl.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("kvPath"))
//        historyDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(configMap("saveHistoryPath"))

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
                    } else {
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
//      deleteGolbalFile(filePath)
        kv.saveAsTextFile(filePath+s"_ChangeTag_${timeStr}")
    }
    def run_scrape_from_acc(prjName:String, method:String, dateStr:String,tagName:String,enc: Enc): Unit ={
        val cfgs = getConfig(spark,prjName,method,dateStr)
        if (method.endsWith("_deprecated"))
            scrapeScourceAcc_(cfgs,enc)
        else scrapeSourceAcc(cfgs,dateStr,enc)
        // get method first part
        val mtd = method.split("_")(0)
        if (mtd == "pv"){
            processPv(cfgs,enc)
        }else if(mtd == "acc"){
            processAcc(cfgs,enc)
        }else{
            throw new Exception("method not allowed")
        }

        matchPortal(cfgs)
        dropHistory(prjName,tagName,cfgs)
        kvTag(tagName,cfgs)
    }

    def run_scrape_from_pv(prjName: String, method: String, dateStr: String, tagName: String, enc: Enc): Unit ={
        val cfgs = getConfig(spark,prjName,method,dateStr)
        scrapeSourcePV(cfgs,enc)
        if (method == "pv"){
            processPv(cfgs,enc)
        }else if(method == "acc") {
            processAcc(cfgs, enc)
        } else if(method == "domain"){
            processPv(cfgs,enc)
        }else{
          throw new Exception("method not allowed")
        }

        matchPortal(cfgs)
        dropHistory(prjName,tagName,cfgs)
        kvTag(tagName,cfgs)
    }
    def run_process_acc(prjName:String,method:String,dateStr:String, tagName:String,enc: Enc): Unit ={
        val cfgs = getConfig(spark,prjName,method,dateStr)
        deleteGolbalFile(cfgs("processPath"))
        deleteGolbalFile(cfgs("matchPortalPath"))
        deleteGolbalFile(cfgs("dropHistoryPath"))
        deleteGolbalFile(cfgs("kvPath"))
        deleteGolbalFile(cfgs("saveHistoryPath"))

        processAcc(cfgs,enc)

        matchPortal(cfgs)
        dropHistory(prjName,tagName,cfgs)
        kvTag(tagName,cfgs)
    }
    def run_process_pv(prjName:String,method:String,dateStr:String, tagName:String,enc: Enc): Unit ={
        val cfgs = getConfig(spark,prjName,method,dateStr)
        deleteGolbalFile(cfgs("processPath"))
        deleteGolbalFile(cfgs("matchPortalPath"))
        deleteGolbalFile(cfgs("dropHistoryPath"))
        deleteGolbalFile(cfgs("kvPath"))
        deleteGolbalFile(cfgs("saveHistoryPath"))

        processPv(cfgs,enc)

        matchPortal(cfgs)
        dropHistory(prjName,tagName,cfgs)
        kvTag(tagName,cfgs)
    }

    def run_process(prjName:String,method:String,dateStr:String, tagName:String,enc: Enc): Unit ={
        val cfgs = getConfig(spark,prjName,method,dateStr)
        deleteGolbalFile(cfgs("processPath"))
        deleteGolbalFile(cfgs("matchPortalPath"))
        deleteGolbalFile(cfgs("dropHistoryPath"))
        deleteGolbalFile(cfgs("kvPath"))
        deleteGolbalFile(cfgs("saveHistoryPath"))

        // get method first part
        val mtd = method.split("_")(0)
        if (mtd == "pv"){
            processPv(cfgs,enc)
        }else if(mtd == "acc"){
            processAcc(cfgs,enc)
        }else{
            throw new Exception("method not allowed")
        }

        matchPortal(cfgs)
        dropHistory(prjName,tagName,cfgs)
        kvTag(tagName,cfgs)
    }


    def scrape_lte(prjName:String, method:String, dateStr:String, tagName:String, enc: Enc): Unit = {
        val cfgs = getConfig(spark,prjName,method,dateStr)
        val lte = new ScrapeLTE(spark,cfgs)
        lte.scrape(dateStr,enc)
        lte.process(enc)
        lte.dropHistory()
        lte.kv(tagName,method,dateStr)
    }
    def process_lte(prjName:String, method:String, dateStr:String, tagName:String, enc: Enc): Unit = {
        val cfgs = getConfig(spark,prjName,method,dateStr)
        val lte = new ScrapeLTE(spark,cfgs)
        deleteGolbalFile(cfgs("processPath"))
        deleteGolbalFile(cfgs("matchPortalPath"))
        deleteGolbalFile(cfgs("dropHistoryPath"))
        deleteGolbalFile(cfgs("kvPath"))
        deleteGolbalFile(cfgs("saveHistoryPath"))

        lte.process(enc)
        lte.dropHistory()
        lte.kv(tagName,method,dateStr)
    }
    def process_cdpi(prjName:String, method:String, dateStr:String, tagName:String, enc: Enc): Unit = {
        val cfgs = getConfig(spark,prjName,method,dateStr)
        val cdpi = new ScrapeCDPI(spark,cfgs)
        deleteGolbalFile(cfgs("processPath"))
        deleteGolbalFile(cfgs("matchPortalPath"))
        deleteGolbalFile(cfgs("dropHistoryPath"))
        deleteGolbalFile(cfgs("kvPath"))
        deleteGolbalFile(cfgs("saveHistoryPath"))

        cdpi.process(enc)
        cdpi.dropHistory()
        cdpi.kv(tagName)
    }

    def scrape_cdpi(prjName:String, method:String, dateStr:String, tagName:String, enc: Enc): Unit = {
        val cfgs = getConfig(spark,prjName,method,dateStr)
        val cdpi = new ScrapeCDPI(spark,cfgs)
        cdpi.scrape(dateStr,enc)
        cdpi.process(enc)
        cdpi.dropHistory()
        cdpi.kv(tagName)
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
            case ("test_conf",prj,mth,ds) => getConfig(spark,prj,mth,ds)
            case ("change_tag",arg1,arg2,arg3) => change_tag(arg1,arg2,arg3,args.lift(4).getOrElse(""))
            case("scrape_acc",arg1,arg2,arg3) => run_scrape_from_acc(arg1,arg2,arg3, args.lift(4).getOrElse(""), enc)
            case("run_scrape_from_pv",arg1,arg2,arg3) => run_scrape_from_pv(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
    //      case("run_process",arg1,"pv",arg3) => run_process_pv(arg1,"pv",arg3,args.lift(4).getOrElse(""),enc)
            case("run_process",arg1,arg2,arg3) => run_process(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
    //      case("run_process",arg1,"acc",arg3) =>run_process_acc(arg1,"acc",arg3,args.lift(4).getOrElse(""),enc)
            case("scrape_lte",arg1,arg2,arg3) => scrape_lte(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
            case("scrape_cdpi",arg1,arg2,arg3)=>scrape_cdpi(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
            case("process_cdpi",arg1,arg2,arg3)=>process_cdpi(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
            case("process_lte",arg1,arg2,arg3)=>process_lte(arg1,arg2,arg3,args.lift(4).getOrElse(""),enc)
        }
    }



}
