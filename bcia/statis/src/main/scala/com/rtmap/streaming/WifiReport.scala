package com.rtmap.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import com.rtmap.utils.{ConfUtil, PolyUtil}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

/**
 * Created by skp on 2015/10/9.
 * spark-submit  --class com.rtmap.streaming.WifiReport --master spark://r2s5:7077 --executor-memory 512M --total-executor-cores 1 airport-1.0-SNAPSHOT.jar hdfs://r2s5/tmp/wifi/
 */

object WifiReport {
  def main(args: Array[String]) {

    //val sparkConf = new SparkConf().setAppName("WifiReport").setMaster("local[2]")
    val sparkConf = new SparkConf().setAppName("WifiReport")
    val ssc =  new StreamingContext(sparkConf, Seconds(args(0).toInt))
    ssc.checkpoint("checkpoint_lbs")

    val re_time=300

    val formats = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val topicpMap =args(1).split(",").map((_,args(3).toInt)).toMap
    //val lines = KafkaUtils.createStream(ssc, args(2), args(4), topicpMap,StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    val lines = KafkaUtils.createStream(ssc, args(2), args(4), topicpMap).map(_._2)

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
      jedis.select(3)
      val s = jedis.keys("*").toArray().map(p => jedis.get(p.toString))
      //jedis.flushDB()
      InternalRedisClient.getPool.returnResource(jedis)
      InternalRedisClient.getPool.destroy()
      s
    }

    lines.flatMap(_.split("\n").map(_.split("\t")).filter(_.length==8))
      .foreachRDD(m => {m.foreachPartition(l => l.foreach(p => {
      println(p(7))
      object InternalRedisClient extends Serializable {
        @transient private var pool: JedisPool = null
        def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
          makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 100000)}

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
      jedis.select(0)
      val times = formats.parse(p(0)).getTime/1000
      val segm =p(7).substring(0,15)+"0"
      if (PolyUtil.isInsidePolygon((p(4).toInt,p(5).toInt),ConfUtil.poly) == 1) {
         //println(p(0),p(1),p(4),p(5))//时间,mac,x,y
        if (jedis.exists(p(1)) == true){
          val time1 = jedis.get(p(1)).split("\t")(2)
          //jedis.setex(p(1),(args(5).toInt-times.toInt+time1.toInt).abs,p(1)+"\t"+segm+"\t"+time1+"\t"+(times.toInt-time1.toInt).abs.toString)
          jedis.setex(p(1),args(5).toInt,p(1)+"\t"+segm+"\t"+time1+"\t"+(times.toInt-time1.toInt).abs.toString)
        } //mac,时段,时间1,dura
        else {jedis.setex(p(1),args(5).toInt,p(1)+"\t"+segm+"\t"+times+"\t"+"0")}//mac,时段,时间1,0
      } else {
        if (jedis.exists(p(1)) == true) {
          val dura = jedis.get(p(1)).split("\t")(3).toInt
          jedis.select(3)
          if (dura>15 & dura<3600){
            if (jedis.exists(segm) == true) {
              val l = jedis.get(segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex(segm,re_time,"se"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt) {jedis.setex(segm,re_time,"se"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex(segm,re_time,"se"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex(segm,re_time,"se"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
          }

          if (dura>15 & dura<=240) {
            if (jedis.exists("1"+"-"+segm) == true) {
              val l = jedis.get("1"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("1"+"-"+segm,re_time,"1"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("1"+"-"+segm,re_time,"1"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("1"+"-"+segm,re_time,"1"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("1"+"-"+segm,re_time,"1"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
          else if (dura>240 & dura<=360) {
            if (jedis.exists("2"+"-"+segm) == true) {
              val l = jedis.get("2"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("2"+"-"+segm,re_time,"2"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("2"+"-"+segm,re_time,"2"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("2"+"-"+segm,re_time,"2"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("2"+"-"+segm,re_time,"2"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
          else if (dura>360 & dura<=500) {
            if (jedis.exists("3"+"-"+segm) == true) {
              val l = jedis.get("3"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("3"+"-"+segm,re_time,"3"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("3"+"-"+segm,re_time,"3"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("3"+"-"+segm,re_time,"3"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("3"+"-"+segm,re_time,"3"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
          else if (dura>500 & dura<=800) {
            if (jedis.exists("4"+"-"+segm) == true) {
              val l = jedis.get("4"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("4"+"-"+segm,re_time,"4"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("4"+"-"+segm,re_time,"4"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("4"+"-"+segm,re_time,"4"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("4"+"-"+segm,re_time,"4"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
          else if (dura>800 & dura<=1200) {
            if (jedis.exists("5"+"-"+segm) == true) {
              val l = jedis.get("5"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("5"+"-"+segm,re_time,"5"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("5"+"-"+segm,re_time,"5"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("5"+"-"+segm,re_time,"5"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("5"+"-"+segm,re_time,"5"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
          else if (dura>1200 & dura<=3600) {
            if (jedis.exists("6"+"-"+segm) == true) {
              val l = jedis.get("6"+"-"+segm).split("\t")
              if (dura >= l(3).toInt){jedis.setex("6"+"-"+segm,re_time,"6"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else if (dura <= l(2).toInt & dura > 0) {jedis.setex("6"+"-"+segm,re_time,"6"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
              else {jedis.setex("6"+"-"+segm,re_time,"6"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt)+"\t"+(l(5).toInt+1).toString)}
            } else {jedis.setex("6"+"-"+segm,re_time,"6"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}}
        }
        jedis.select(0)
        jedis.del(p(1))
      }//时段,min,max,sum,count

      InternalRedisClient.getPool.returnResource(jedis)
      InternalRedisClient.getPool.destroy()
    }))
    })

    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="se").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(args(8))//时段,min,max,avg,速率(人/分钟)
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)!="se").map(p => p(0)+"\t"+p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(args(9))//分档,时段,min,max,avg,速率(人/分钟)
    if (args(6).toInt == 1) {lines.saveAsTextFiles(args(10))}
    lines.count().foreachRDD(foreachFunc = l => {
      def func(records: Iterator[Long]) {
        var conn: Connection = null
        var stmt1: PreparedStatement = null
        var stmt2: PreparedStatement = null
        try {
          conn = DriverManager.getConnection("jdbc:mysql://r2s4:3306/bcia_statis", "bcia", "bcia")
          val res = result.map(m => (m.split("\t")))
          res.filter(n => n(0) == "se").foreach(p => {
            stmt1 = conn.prepareStatement("insert ignore into wifi_result_segmt(segmt,min_duar,max_duar,avg_duar,pass_rate,median_duar) values (?,?,?,?,?,?)");
            stmt1.setString(1, p(1))
            stmt1.setInt(2, p(2).toInt)
            stmt1.setInt(3, p(3).toInt)
            stmt1.setInt(4, p(4).toInt / p(5).toInt)
            stmt1.setFloat(5, p(5).toFloat / 10)
            stmt1.setString(6, "")
            stmt1.executeUpdate();})
          res.filter(n => n(0) != "se").foreach(p => {
            stmt2 = conn.prepareStatement("insert ignore into wifi_result_segmt_bracket(level_id,segmt,min_duar,max_duar,avg_duar,pass_rate) values (?,?,?,?,?,?)");
            stmt2.setInt(1, p(0).toInt)
            stmt2.setString(2, p(1))
            stmt2.setInt(3, p(2).toInt)
            stmt2.setInt(4, p(3).toInt)
            stmt2.setInt(5, p(4).toInt/p(5).toInt)
            stmt2.setFloat(6, p(5).toFloat/10)
            stmt2.executeUpdate();})
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt1 != null) {stmt1.close()}
          if (stmt2 != null) {stmt2.close()}
          if (conn != null) {conn.close()}}}
      l.foreachPartition(func)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
