package com.javachen.grab.redis

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._


class RedisKRDD(
                 @transient sc: SparkContext,
                 @transient redisHosts: Array[(String, Int, Int, Int)], //last value is number of partitions per host
                 @transient namespace: Int,
                 @transient scanCount: Int,
                 @transient keyPattern: String,
                 @transient makePartitioner: Boolean
                 )
  extends BaseRedisRDD(sc, redisHosts, namespace, scanCount, keyPattern, makePartitioner) with Logging {

  override def compute(split: Partition, context: TaskContext): Iterator[(String, String)] = {
    val partition: RedisPartition = split.asInstanceOf[RedisPartition]
    val endpoint = partition.endpoint
    logDebug("RDD: " + split.index + ", Connecting to: " + endpoint)
    val jedis = new Jedis(endpoint._1.getHostAddress, endpoint._2)
    jedis.select(namespace)
    val keys = getKeys(jedis, keyPattern, scanCount, partition)
    keys.groupBy(JedisClusterCRC16.getSlot(_)).flatMap(x => getVals(jedis, x._2)).iterator
  }

  def getVals(jedis: Jedis, keys: scala.collection.mutable.Set[String]): Seq[(String, String)] = {
    keys.zip(jedis.mget(keys.toArray: _*)).toSeq
  }
}
