package com.javachen.grab.redis

import java.util

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import redis.clients.jedis._

import scala.collection.JavaConversions._


class RedisSRDD(//[K,V]
                @transient sc: SparkContext,
                @transient redisHosts: Array[(String, Int, Int, Int)], //last value is number of partitions per host
                @transient namespace: Int = 0,
                @transient scanCount: Int = 10000,
                @transient keyPattern: String = "*",
                @transient makePartitioner: Boolean,
                val valuePattern: String = "*"
                 )
  extends BaseRedisRDD(sc, redisHosts, namespace, scanCount, keyPattern, makePartitioner) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress, endpoint._2)
    jedis.select(namespace)
    val keys = getKeys(jedis, keyPattern, scanCount, partition)
    keys.flatMap(getVals(jedis, _, valuePattern, scanCount)).iterator
  }

  def getVals(jedis: Jedis, k: String, valuePattern: String, scanCount: Int): Seq[(String, String)] = {
    val params = new ScanParams().`match`(valuePattern).count(scanCount)
    val vals = new util.HashSet[String]()
    var scan = jedis.sscan(k, "0", params)
    vals.addAll(scan.getResult)
    while (scan.getStringCursor != "0") {
      scan = jedis.sscan(k, scan.getStringCursor, params)
      vals.addAll(scan.getResult)
    }
    Seq.fill(vals.size) {
      k
    }.zip(vals)
  }

}