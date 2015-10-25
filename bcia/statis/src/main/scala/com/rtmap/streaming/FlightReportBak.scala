package com.rtmap.streaming

/**
 * Created by skp on 2015/10/17.
 */

import java.text.SimpleDateFormat

import com.rtmap.utils.ConfUtil
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

/**
 * Created by skp on 2015/10/9.
 */
object FlightReportBak {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("FlightReport").setMaster("local[2]")
    //val sc  = new SparkContext(sparkConf)
    val ssc =  new StreamingContext(sparkConf, Seconds(30))
    ssc.checkpoint("checkpoint")
    // val sqc = new SQLContext(sc)

    val formats = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    //val topicpMap =args(1).split(",").map((_,args(2).toInt)).toMap
    val topicpMap =ConfUtil.flightReportTopics.split(",").map((_,ConfUtil.flightReportNumThreads)).toMap
    val lines = KafkaUtils.createStream(ssc,ConfUtil.flightReportZkQuorum, ConfUtil.flightReportGroup, topicpMap).map(_._2.substring(15))
    val lines_lkxxb = lines.filter(l => l.indexOf("lkxxb") == 0)
    val lines_barcode = lines.filter(l => l.indexOf("barcode") == 0)
    val lines_ajxxb = lines.filter(l => l.indexOf("ajxxb") == 0)

    lines_lkxxb.map(_.split("\t"))//2:旅客id,3:航班号,5:座位号
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1",6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(1)
      jedis.setex(m(3)+"-"+m(5),3600,m(2))
      RedisClient.pool.returnResource(jedis)
    }))})

    lines_barcode.map(_.split("\t"))//4:航班号,7:座位号,14:最后一次扫描时间
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1",6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(1)
      //jedis.setex( m(4) + "-" + m(7),120,m(14))
      if (jedis.exists(m(4)+"-"+m(7)) == 1) {
        val key1 = jedis.get(m(4) + "-" + m(7))
        jedis.setex(key1,3600,m(14))
        jedis.del(m(4) + "-" + m(7))
        RedisClient.pool.returnResource(jedis)
      }
    }))})

    lines_ajxxb.map(_.split("\t"))//3:旅客id,5:安检通道号,7:安检时间
      .foreachRDD(l => {l.foreachPartition(p => p.foreach(m => {
      object RedisClient extends Serializable {
        lazy val pool = new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1",6379,30000)
        lazy val hook = new Thread {
          override def run = { println("Execute hook thread: " + this)
            pool.destroy()}}
        sys.addShutdownHook(hook.run)}
      val jedis =RedisClient.pool.getResource
      jedis.select(1)
      //jedis.setex(m(3),120,m(5)+"\t"+m(7))
      if (jedis.exists(m(3)) == 1) {
        val b_time = formats.parse(jedis.get(m(3))).getTime/1000
        val a_time  = formats.parse(m(7)).getTime/1000
        val anjian_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val segm =anjian_time.format(a_time).substring(0,15)
        val dura = a_time-b_time
        val re = (m(3),m(5),segm,a_time-b_time,anjian_time) //(旅客id,安检通道号,安检时间段,排队时长,安检时间)
        val key1 = m(5)+"-"+segm
        jedis.del(m(3))
        jedis.select(2)
        //jedis.setex(m(3),30,m(3)+"\t"+m(5)+"\t"+segm+"\t"+dura+"\t"+anjian_time)
        if (jedis.exists(key1) == 1) {
          val l = jedis.get(key1).split("\t")
          if (dura >= l(2).toInt){
            jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+l(2)+"\t"+dura+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)
          } else {jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+dura+"\t"+l(2)+"\t"+((dura+l(4).toInt)/(l(5).toInt+1)).toString+"\t"+(dura+l(4).toInt).toString+"\t"+(l(5).toInt+1).toString)}
        } else {jedis.setex(key1,30,m(5)+"\t"+segm+"\t"+dura+"\t"+"0"+"dura"+"\t"+"\t"+dura+"\t"+"1")}
        //安检通道号,安检时间段,排队时长(min),排队时长(max),avg(dura),sum(dura),count
        RedisClient.pool.returnResource(jedis)
      }
    }))})
    println("bbbbbbbbbbbbbbb")
    object RedisClient extends Serializable {
      lazy val pool = new JedisPool(new GenericObjectPoolConfig(),"127.0.0.1",6379,30000)
      lazy val hook = new Thread {
        override def run = { println("Execute hook thread: " + this)
          pool.destroy()}}
      sys.addShutdownHook(hook.run)}
    val jedis =RedisClient.pool.getResource
    jedis.select(1)
    val s = jedis.keys("*").toArray().map(p => jedis.get(p.toString).split("\t"))
      .map(p => p(0))
    //.map(p => (p(0),p(1),p(2),p(3),p(4)))
    println("aaaaaaaaaaaaaa")
    println(s.take(10))
    RedisClient.pool.returnResource(jedis)
    println("cccccccccccccc")
    // val hdfs_path = args(7).split(",")
    // if (args(6).toInt == 1) {
    //      lines_lkxxb.saveAsTextFiles(hdfs_path(0))
    //      lines_barcode.saveAsTextFiles(hdfs_path(1))
    //      lines_ajxxb.saveAsTextFiles(hdfs_path(2))}
    ssc.start()
    ssc.awaitTermination()
  }

}
