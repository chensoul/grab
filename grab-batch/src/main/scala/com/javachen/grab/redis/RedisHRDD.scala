package com.javachen.grab.redis

import org.apache.spark._
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._


class RedisHRDD(
                 @transient sc: SparkContext,
                 @transient redisHosts: Array[(String, Int, Int, Int)], //last value is number of partitions per host
                 @transient namespace: Int,
                 @transient scanCount: Int,
                 @transient keyPattern: String,
                 val checkForKeyType: Boolean = false
                 )
  extends BaseRedisRDD(sc, redisHosts, namespace, scanCount, keyPattern, false) with Logging {


  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress, endpoint._2)
    jedis.select(namespace)
    val keys = getKeys(jedis, keyPattern, scanCount, partition)
    logDebug("Keys found: " + keys.mkString(" "))
    keys.flatMap(k => if (!checkForKeyType || jedis.`type`(k) == "hash") jedis.hgetAll(k) else Seq()).iterator
  }
}