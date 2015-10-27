package com.rtmap.streaming

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

     lazy val result = {object RedisClient extends Serializable {
      lazy val pool = new JedisPool(new GenericObjectPoolConfig(),args(7),6379,30000)
      lazy val hook = new Thread {
        override def run = { println("Execute hook thread: " + this)
          pool.destroy()}}
      sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(2)
      val s = jedis.keys("*").toArray()
        .map(p => {jedis.get(p.toString)})
      RedisClient.pool.returnResource(jedis)
       RedisClient.pool.destroy()
      s
    }

    lines_lkxxb.map(_.split("\t"))//2:旅客id,3:航班号,8:登机号
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),args(7),6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(1)
      jedis.setex(m(3)+"-"+m(8),args(6).toInt,m(2))
      RedisClient.pool.returnResource(jedis)
      RedisClient.pool.destroy()
    }))})

    lines_barcode.map(_.split("\t"))//4:航班号,8:登机序号,14:最后一次扫描时间
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),args(7),6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(1)
      if (jedis.exists(m(4)+"-"+m(8)) == true) {
        val key1 = jedis.get(m(4) + "-" + m(8))
        jedis.select(4)
        jedis.setex(key1,43200,m(14))
        jedis.select(1)
        jedis.del(m(4) + "-" + m(8))
      }
      RedisClient.pool.returnResource(jedis)
      RedisClient.pool.destroy()
    }))})

    lines_ajxxb.map(_.split("\t"))//3:旅客id,5:安检通道号,7:安检时间
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),args(7),6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(4)
      if (jedis.exists(m(3)) == true) {
        val b_time = formats.parse(jedis.get(m(3))).getTime/1000
        val a_time  = formats.parse(m(7)).getTime/1000
        val segm =m(7).substring(0,15)
        val dura = a_time-b_time
        jedis.select(2)
        if (jedis.exists(segm) == true) {
             val l = jedis.get(segm).split("\t")
             if (dura >= l(3).toInt & dura<=3600){jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+l(2)+"\t"+dura.toString+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
             else if (dura <= l(2).toInt & dura>0) {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+dura.toString+"\t"+l(3)+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
             else {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
        } else {
          if (dura>0 & dura<=3600)
          {jedis.setex(segm,re_time,"sg"+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
        } //安检时间段,排队时长(min),排队时长(max),sum(dura),count
        if (jedis.exists(segm+"-"+m(5)) == true) {
          val l = jedis.get(segm+"-"+m(5)).split("\t")
          if (dura >= l(4).toInt & dura <= 3600){jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+l(3)+"\t"+dura.toString+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
          else if (dura <= l(3).toInt & dura > 0) {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+dura.toString+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
          else {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+l(3)+"\t"+l(4)+"\t"+(dura+l(5).toInt).toString+"\t"+(l(6).toInt+1).toString)}
        } else {
          if (dura>0 & dura<=3600)
          {jedis.setex(segm+"-"+m(5),re_time,"sd"+"\t"+m(5)+"\t"+segm+"\t"+dura.toString+"\t"+dura.toString+"\t"+dura.toString+"\t"+"1")}
        } //安检通道号,安检时间段,排队时长(min),排队时长(max),sum(dura),count

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
      RedisClient.pool.returnResource(jedis)
      RedisClient.pool.destroy()
    }))})

    val hdfs_path=args(8).split(" ")
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sg").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(hdfs_path(0))
    //lines.flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sg").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+(p(4).toInt/p(5).toInt).toString+"\t"+(p(5).toFloat/10).toString)).saveAsTextFiles(hdfs_path(0))//时段,min,max,avg,速率(人/分钟)
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="sd").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+p(4)+"\t"+(p(5).toInt/p(6).toInt).toString+"\t"+(p(6).toFloat/10).toString+"\t"+(p(5).toFloat/p(6).toFloat/480).toString)).saveAsTextFiles(hdfs_path(1))//安检通道号,时段,min,max,avg,速率,负荷率
    lines.count().flatMap(l => result.map(m => (m.split("\t"))).filter(p => p(0)=="le").map(p => p(1)+"\t"+p(2)+"\t"+p(3)+"\t"+p(4)+"\t"+(p(5).toInt/p(6).toInt).toString+"\t"+(p(6).toFloat/10).toString)).saveAsTextFiles(hdfs_path(2))//档次,时段,min,max,avg,速率

    if (args(5).toInt == 1) {
      lines_lkxxb.saveAsTextFiles(hdfs_path(3))
      lines_barcode.saveAsTextFiles(hdfs_path(4))
      lines_ajxxb.saveAsTextFiles(hdfs_path(5))}

   // lines.map(_.split("\t"))
   // .foreachRDD(l => {l.foreach(m => {
   //   object RedisClient extends Serializable {
   //     lazy val pool = new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1",6379,30000)
   //     lazy val hook = new Thread {
   //       override def run = { println("Execute hook thread: " + this)
   //         pool.destroy()}}
   //     sys.addShutdownHook(hook.run)}
   //   val jedis =RedisClient.pool.getResource
   //   jedis.select(1)
   //   if (m(0) == "lkxxb") {//2:旅客id,3:航班号,5:座位号
   //     jedis.setex(m(3)+"-"+m(5),3600,m(2))
   //   }
   //   else if (m(0) == "barcode") {//4:航班号,7:座位号,14:最后一次扫描时间
   //     if (jedis.exists(m(4)+"-"+m(7)) == 1) {
   //       val key1 = jedis.get(m(4) + "-" + m(7))
   //       jedis.setex(key1,3600,m(14))
   //       jedis.del(m(4) + "-" + m(7))}
   //   }
   //   else if (m(0) == "ajxxb") {//3:旅客id,5:安检通道号,7:安检时间
   //     if (jedis.exists(m(3)) == 1) {
   //       val b_time = formats.parse(jedis.get(m(3))).getTime/1000
   //       val a_time  = formats.parse(m(7)).getTime/1000
   //       val anjian_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
   //       val segm =anjian_time.format(a_time).substring(0,15)
   //       val dura = a_time-b_time
   //       val re = (m(3),m(5),segm,a_time-b_time,anjian_time) //(旅客id,安检通道号,安检时间段,排队时长,安检时间)
   //       val key1 = m(5)+"-"+segm
   //       jedis.del(m(3))
   //       jedis.select(2)
   //       if (jedis.exists(key1) == 1) {
   //         val l = jedis.get(key1).split("\t")
   //         if (dura >= l(3).toInt){
   //           jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+l(2)+"\t"+dura+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)
   //         } else if (dura <= l(2).toInt) {jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+dura+"\t"+l(3)+"\t"+((dura+l(4).toInt)/(l(5).toInt+1)).toString+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
   //         else {jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+l(2)+"\t"+l(3)+"\t"+((dura+l(4).toInt)/(l(5).toInt+1)).toString+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
   //       } else {jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+dura+"\t"+"0"+dura+"\t"+"\t"+dura+"\t"+"1")}
   //       //安检通道号,安检时间段,排队时长(min),排队时长(max),avg(dura),sum(dura),count
   //     }
   //   }
   //   RedisClient.pool.returnResource(jedis)
   // })})
    ssc.start()
    ssc.awaitTermination()
  }

}
