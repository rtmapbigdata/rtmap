package com.rtmap.streaming

import java.sql.{DriverManager, Connection, PreparedStatement}
import java.text.SimpleDateFormat

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

/**
 * Created by skp on 2015/10/9.
 *  spark-submit  --class com.rtmap.streaming.FlightReport --master spark://r2s5:7077 --executor-memory 512M --total-executor-cores 1 airport-1.0-SNAPSHOT.jar hdfs://r2s5/tmp/lkxxb/
 */
object FlightReport {
  def main(args: Array[String]) {

    //val sparkConf = new SparkConf().setAppName("FlightReport").setMaster("local[2]")
    val sparkConf = new SparkConf().setAppName("FlightReport")
    //val sc  = new SparkContext(sparkConf)
    val ssc =  new StreamingContext(sparkConf, Seconds(args(0).toInt))
    ssc.checkpoint("checkpoint")
   // val sqc = new SQLContext(sc)

    val formats = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    //val topicpMap =args(1).split(",").map((_,args(2).toInt)).toMap
    val topicpMap =args(1).split(",").map((_,args(3).toInt)).toMap
    val lines = KafkaUtils.createStream(ssc,args(2), args(4), topicpMap).map(_._2.substring(15))
    lazy val lines_lkxxb = lines.filter(l => l.indexOf("lkxxb") == 0)
    lazy val lines_barcode = lines.filter(l => l.indexOf("barcode") == 0)
    lazy val lines_ajxxb = lines.filter(l => l.indexOf("ajxxb") == 0)

    val re_time=300

     lazy val result = {
       object InternalRedisClient extends Serializable {
         @transient private var pool: JedisPool = null
         def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
           makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)}

         def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
           if(pool == null) {
             val poolConfig = new GenericObjectPoolConfig()
             poolConfig.setMaxTotal(maxTotal)
             poolConfig.setMaxIdle(maxIdle)
             poolConfig.setMinIdle(minIdle)
             poolConfig.setTestOnBorrow(testOnBorrow)
             poolConfig.setTestOnReturn(testOnReturn)
             poolConfig.setMaxWaitMillis(maxWaitMillis)
             pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

             val hook = new Thread{
               override def run = pool.destroy()}
             //sys.addShutdownHook(hook.run)
           }}

         def getPool: JedisPool = {
           assert(pool != null)
           pool}}

       // Redis configurations
       val maxTotal = 500000
       val maxIdle = 500000
       val minIdle = 1
       val redisHost = args(7)
       val redisPort = 6379
       val redisTimeout = 10000
       InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

       val jedis =InternalRedisClient.getPool.getResource
      jedis.select(2)
      val s = jedis.keys("*").toArray()
        .map(p => {jedis.get(p.toString)})
       InternalRedisClient.getPool.returnResource(jedis)
       InternalRedisClient.getPool.destroy()
      s
    }

    lines_lkxxb.map(_.split("\t"))//2:旅客id,3:航班号,8:登机号,4:航班日期
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object InternalRedisClient extends Serializable {
        @transient private var pool: JedisPool = null
        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
          makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)}

        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
          if(pool == null) {
            val poolConfig = new GenericObjectPoolConfig()
            poolConfig.setMaxTotal(maxTotal)
            poolConfig.setMaxIdle(maxIdle)
            poolConfig.setMinIdle(minIdle)
            poolConfig.setTestOnBorrow(testOnBorrow)
            poolConfig.setTestOnReturn(testOnReturn)
            poolConfig.setMaxWaitMillis(maxWaitMillis)
            pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

            val hook = new Thread{
              override def run = pool.destroy()}
            //sys.addShutdownHook(hook.run)
          }}

        def getPool: JedisPool = {
          assert(pool != null)
          pool}}

      // Redis configurations
      val maxTotal = 500000
      val maxIdle = 500000
      val minIdle = 1
      val redisHost = args(7)
      val redisPort = 6379
      val redisTimeout = 10000
      InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

      val jedis =InternalRedisClient.getPool.getResource
      jedis.select(1)
      jedis.setex(m(3)+"-"+m(8)+"-"+m(4),args(6).toInt,m(2))
      InternalRedisClient.getPool.returnResource(jedis)
      InternalRedisClient.getPool.destroy()
    }))})

    lines_barcode.map(_.split("\t"))//4:航班号,8:登机序号,5:航班日期14:最后一次扫描时间,11:闸机名称
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object InternalRedisClient extends Serializable {
        @transient private var pool: JedisPool = null
        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
          makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)}

        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
          if(pool == null) {
            val poolConfig = new GenericObjectPoolConfig()
            poolConfig.setMaxTotal(maxTotal)
            poolConfig.setMaxIdle(maxIdle)
            poolConfig.setMinIdle(minIdle)
            poolConfig.setTestOnBorrow(testOnBorrow)
            poolConfig.setTestOnReturn(testOnReturn)
            poolConfig.setMaxWaitMillis(maxWaitMillis)
            pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

            val hook = new Thread{
              override def run = pool.destroy()}
            //sys.addShutdownHook(hook.run)
          }}

        def getPool: JedisPool = {
          assert(pool != null)
          pool}}

      // Redis configurations
      val maxTotal = 500000
      val maxIdle = 500000
      val minIdle = 1
      val redisHost = args(7)
      val redisPort = 6379
      val redisTimeout = 10000
      InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

      val jedis =InternalRedisClient.getPool.getResource
      jedis.select(1)
      if (jedis.exists(m(4)+"-"+m(8)+"-"+m(5)) == true) {
        val key1 = jedis.get(m(4) + "-" + m(8)+"-"+m(5))
        jedis.select(4)
        jedis.setex(key1,43200,m(14)+"\t"+m(11))
        jedis.select(1)
        jedis.del(m(4) + "-" + m(8)+"-"+m(5))
      }
      InternalRedisClient.getPool.returnResource(jedis)
      InternalRedisClient.getPool.destroy()
    }))})

    lines_ajxxb.map(_.split("\t"))//3:旅客id,5:安检通道号,7:安检时间
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object InternalRedisClient extends Serializable {
        @transient private var pool: JedisPool = null
        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
          makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)}

        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
          if(pool == null) {
            val poolConfig = new GenericObjectPoolConfig()
            poolConfig.setMaxTotal(maxTotal)
            poolConfig.setMaxIdle(maxIdle)
            poolConfig.setMinIdle(minIdle)
            poolConfig.setTestOnBorrow(testOnBorrow)
            poolConfig.setTestOnReturn(testOnReturn)
            poolConfig.setMaxWaitMillis(maxWaitMillis)
            pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

            val hook = new Thread{
              override def run = pool.destroy()}
            //sys.addShutdownHook(hook.run)
          }}

        def getPool: JedisPool = {
          assert(pool != null)
          pool}}

      // Redis configurations
      val maxTotal = 500000
      val maxIdle = 500000
      val minIdle = 1
      val redisHost = args(7)
      val redisPort = 6379
      val redisTimeout = 10000
      InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

      val jedis =InternalRedisClient.getPool.getResource
      jedis.select(4)
      if (jedis.exists(m(3)) == true) {
        val barcodes = jedis.get(m(3)).split("\t")(1)
        val b_time = formats.parse(jedis.get(m(3)).split("\t")(0)).getTime/1000
        val a_time  = formats.parse(m(7)).getTime/1000
        val segm =m(18).substring(0,15)+"0"
        val dura = a_time-b_time-10
        jedis.select(2)
        if (jedis.exists(segm) == true & dura>0 & dura<=3600) {
             val l = jedis.get(segm).split("\t")
             if (dura >= l(3).toInt & dura<=3600){jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
             else if (dura <= l(2).toInt & dura>0) {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
             else {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
        } else {
          if (dura>0 & dura<=3600)
          {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
        } //安检时间段,排队时长(min),排队时长(max),sum(dura),count
        if (jedis.exists(segm+"-"+m(5)) == true & dura>0 & dura<=3600) {
          val l = jedis.get(segm+"-"+m(5)).split("\t")
          if (dura >= l(4).toInt & dura <= 3600){jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
          else if (dura <= l(3).toInt & dura > 0) {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
          else {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
        } else {
          if (dura>0 & dura<=3600)
          {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
        } //安检通道号,安检时间段,排队时长(min),排队时长(max),sum(dura),count

        if (jedis.exists(barcodes+"-"+segm) == true) {
          val l = jedis.get(barcodes+"-"+segm).split("\t")
          jedis.setex(barcodes+"-"+segm,re_time,"barcode"+"\t"+barcodes+"\t"+segm+"\t"+(l(3).toInt+1).toString)
        } else {
          jedis.setex(barcodes+"-"+segm,re_time,"barcode"+"\t"+barcodes+"\t"+segm+"\t"+"1")
        } //闸机口,时间段,count

        if (dura>10 & dura<=240) {
          if (jedis.exists("1"+"-"+segm) == true) {
            val l = jedis.get("1"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("1"+"-"+segm,re_time,"le"+"\t"+"1"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("1"+"-"+segm,re_time,"le"+"\t"+"1"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("1"+"-"+segm,re_time,"le"+"\t"+"1"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("1"+"-"+segm,re_time,"le"+"\t"+"1"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        else if (dura>240 & dura<=360) {
          if (jedis.exists("2"+"-"+segm) == true) {
            val l = jedis.get("2"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("2"+"-"+segm,re_time,"le"+"\t"+"2"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("2"+"-"+segm,re_time,"le"+"\t"+"2"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("2"+"-"+segm,re_time,"le"+"\t"+"2"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("2"+"-"+segm,re_time,"le"+"\t"+"2"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        else if (dura>360 & dura<=500) {
          if (jedis.exists("3"+"-"+segm) == true) {
            val l = jedis.get("3"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("3"+"-"+segm,re_time,"le"+"\t"+"3"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("3"+"-"+segm,re_time,"le"+"\t"+"3"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("3"+"-"+segm,re_time,"le"+"\t"+"3"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("3"+"-"+segm,re_time,"le"+"\t"+"3"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        else if (dura>500 & dura<=800) {
          if (jedis.exists("4"+"-"+segm) == true) {
            val l = jedis.get("4"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("4"+"-"+segm,re_time,"le"+"\t"+"4"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("4"+"-"+segm,re_time,"le"+"\t"+"4"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("4"+"-"+segm,re_time,"le"+"\t"+"4"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("4"+"-"+segm,re_time,"le"+"\t"+"4"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        else if (dura>800 & dura<=1200) {
          if (jedis.exists("5"+"-"+segm) == true) {
            val l = jedis.get("5"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("5"+"-"+segm,re_time,"le"+"\t"+"5"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("5"+"-"+segm,re_time,"le"+"\t"+"5"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("5"+"-"+segm,re_time,"le"+"\t"+"5"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("5"+"-"+segm,re_time,"le"+"\t"+"5"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        else if (dura>1200 & dura<=3600) {
          if (jedis.exists("6"+"-"+segm) == true) {
            val l = jedis.get("6"+"-"+segm).split("\t")
            if (dura >= l(4).toInt){jedis.setex("6"+"-"+segm,re_time,"le"+"\t"+"6"+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else if (dura <= l(3).toInt | l(3).toInt==0) {jedis.setex("6"+"-"+segm,re_time,"le"+"\t"+"6"+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
            else {jedis.setex("6"+"-"+segm,re_time,"le"+"\t"+"6"+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}}
          else {jedis.setex("6"+"-"+segm,re_time,"le"+"\t"+"6"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
        } //分档,安检时间段,排队时长(min),排队时长(max),sum(dura),count
      }
      jedis.select(4)
      jedis.del(m(3))
      InternalRedisClient.getPool.returnResource(jedis)
      InternalRedisClient.getPool.destroy()
    }))})

    val hdfs_path=args(8).split(" ")
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sg").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(hdfs_path(0))
    //lines.flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sg").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(hdfs_path(0))//时段,min,max,avg,速率(人/分钟)
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sd").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+p(4)+"\t"+(p(5).toInt/p(6).toInt).toString+"\t"+(p(6).toFloat/10).toString+"\t"+(p(5).toFloat/p(6).toFloat/480).toString)).saveAsTextFiles(hdfs_path(1))//安检通道号,时段,min,max,avg,速率,负荷率
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="le").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+p(4)+"\t"+(p(5).toInt/p(6).toInt).toString+"\t"+(p(6).toFloat/10).toString)).saveAsTextFiles(hdfs_path(2))//档次,时段,min,max,avg,速率
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="barcode").map(p => p(1)+"\t"+p(2)+"\t"+p(3))).saveAsTextFiles(hdfs_path(3))//闸机口,时段,人数
  lines.count().foreachRDD(l => {
    def func(records: Iterator[Long]) {
      var conn: Connection = null
      var stmt1: PreparedStatement = null
      var stmt2: PreparedStatement = null
      var stmt3: PreparedStatement = null
      var stmt4: PreparedStatement = null
      try {
        conn = DriverManager.getConnection("jdbc:mysql://r2s4:3306/bcia_statis", "bcia", "bcia")
        val res = result.map(m => (m.split("\t")))
        res.filter(n => n(0)=="sg").foreach(p => {
          stmt1 = conn.prepareStatement("insert ignore into profession_result_segmt(segmt,min_duar,max_duar,avg_duar,pass_rate,median_duar) values (?,?,?,?,?,?)");
          stmt1.setString(1,p(1))
          stmt1.setInt(2, p(2).toInt)
          stmt1.setInt(3, p(3).toInt)
          stmt1.setInt(4, p(4).toInt/p(5).toInt)
          stmt1.setFloat(5, p(5).toFloat/10)
          stmt1.setString(6,"")
          stmt1.executeUpdate();})
        res.filter(n => n(0)=="sd").foreach(p => {
          stmt2 = conn.prepareStatement("insert ignore into profession_result_segmt_sck(sck_field,segmt,min_duar,max_duar,avg_duar,pass_rate,load_rate,median_duar,top5_avg_duar) values (?,?,?,?,?,?,?,?,?)");
          stmt2.setString(1,p(1))
          stmt2.setString(2, p(2))
          stmt2.setInt(3,p(3).toInt)
          stmt2.setInt(4,p(4).toInt)
          stmt2.setInt(5,p(5).toInt/p(6).toInt)
          stmt2.setFloat(6,p(6).toFloat/10)
          stmt2.setString(7,(p(5).toFloat/p(6).toFloat/480).toString)
          stmt2.setString(8,"")
          stmt2.setString(9,"")
          stmt2.executeUpdate();})
        res.filter(n => n(0)=="le").foreach(p => {
          stmt3 = conn.prepareStatement("insert ignore into profession_result_segmt_bracket(level_id,segmt,min_duar,max_duar,avg_duar,pass_rate) values (?,?,?,?,?,?)");
          stmt3.setInt(1,p(1).toInt)
          stmt3.setString(2, p(2))
          stmt3.setInt(3, p(3).toInt)
          stmt3.setInt(4, p(4).toInt)
          stmt3.setInt(5, p(5).toInt/p(6).toInt)
          stmt3.setFloat(6, p(6).toFloat/10)
          stmt3.executeUpdate();})
        res.filter(n => n(0)=="barcode").foreach(p => {
          stmt4 = conn.prepareStatement("insert ignore into profession_result_segmt_barcode(segmt,gate_id,pass_num) values (?,?,?)");
          stmt4.setString(1,p(2))
          stmt4.setString(2, p(1))
          stmt4.setInt(3, p(3).toInt)
          stmt4.executeUpdate();})
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (stmt1 != null) {stmt1.close()}
        if (stmt2 != null) {stmt2.close()}
        if (stmt3 != null) {stmt3.close()}
        if (stmt4 != null) {stmt4.close()}
        if (conn != null) {conn.close()}}}
    l.foreachPartition(func)
  })

    if (args(5).toInt == 1) {
      lines_lkxxb.saveAsTextFiles(hdfs_path(4))
      lines_barcode.saveAsTextFiles(hdfs_path(5))
      lines_ajxxb.saveAsTextFiles(hdfs_path(6))}

    ssc.start()
    ssc.awaitTermination()
  }

}
