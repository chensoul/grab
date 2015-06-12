package com.javachen.grab.redis

import java.net.InetAddress
import java.util

import org.apache.spark._
import org.apache.spark.rdd.RDD
import redis.clients.jedis._
import redis.clients.util.JedisClusterCRC16

import scala.collection.JavaConversions._

case class RedisPartition(index: Int,
                          endpoint: (InetAddress, Int), mod: Int, modMax: Int) extends Partition

class RedisPartitioner(val redisHosts: Array[(String, Int, Int, Int)]) extends HashPartitioner(redisHosts.length) {

  override def equals(other: Any): Boolean = other match {
    case h: RedisPartitioner => {
      h.redisHosts.diff(redisHosts).length == 0 && redisHosts.diff(h.redisHosts).length == 0
    }
    case _ =>
      false
  }

}

abstract class BaseRedisRDD(
                             @transient sc: SparkContext,
                             @transient val redisHosts: Array[(String, Int, Int, Int)], //last value is number of partitions per host
                             val namespace: Int,
                             val scanCount: Int = 10000,
                             val keyPattern: String,
                             val makePartitioner: Boolean
                             )
  extends RDD[(String, String)](sc, Seq.empty) with Logging {

  override val partitioner: Option[Partitioner] = if (makePartitioner) Some(new RedisPartitioner(redisHosts)) else None

  def getKeys(jedis: Jedis, keyPattern: String, scanCount: Int, partition: RedisPartition) = {
    val params = new ScanParams().`match`(keyPattern).count(scanCount)
    val keys = new util.HashSet[String]()
    var scan = jedis.scan("0", params)
    val f = scan.getResult.filter(s => (JedisClusterCRC16.getCRC16(s) % (partition.modMax + 1)) == partition.mod)
    keys.addAll(f)
    while (scan.getStringCursor != "0") {
      scan = jedis.scan(scan.getStringCursor, params)
      val f1 = scan.getResult.filter(s => (JedisClusterCRC16.getSlot(s) % (partition.modMax + 1)) == partition.mod)
      keys.addAll(f1)
    }
    keys
  }

  /**
   * Functional implementation of getKeys
   * @param jedis
   * @param keyPattern
   * @param scanCount
   * @param partition
   * @return
   */
  def getKeysRecursive(jedis: Jedis, keyPattern: String, scanCount: Int, partition: RedisPartition) = {
    def getKeysSubset(currentkeys: Seq[String], jedis: Jedis, params: ScanParams, partition: RedisPartition, nextCursor: String, first: Boolean): Seq[String] = {
      if (!first && nextCursor == "0") return currentkeys
      val res = jedis.scan(nextCursor, params)
      getKeysSubset(currentkeys ++ res.getResult, jedis, params, partition, res.getStringCursor, false)
    }
    val params = new ScanParams().`match`(keyPattern).count(scanCount)
    getKeysSubset(Seq[String](), jedis, params, partition, "0", true).distinct
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[RedisPartition].endpoint._1.getHostName)
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until redisHosts.size).map(i => {
      new RedisPartition(i, (InetAddress.getByName(redisHosts(i)._1), redisHosts(i)._2), redisHosts(i)._3, redisHosts(i)._4).asInstanceOf[Partition]
    }).toArray
  }
}


class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  /**
   * @param initialHost This is a tuple of any host and port in the cluster
   * @param numPaprtitionsPerNode Optional, number of partitions per (master) node, default 1
   * @param useSlaves Optional, reserved till when Jedis supports reading slaves in the Cluster, default 0
   * @param namespace Optional, reserved till when Redis supports namespaces in Cluster, default 0
   * @param scanCount Optional, allows to change number of keys returned in each scan iteration
   * @param keyPattern Optional, allows filtering by key on redis, before sending data to Spark
   * @param checkForKeyType Optionally allws to check if content of the set is Hash (very slow)
   * @return Pair RDD of simple Key/Has members stored currently in Redis cluster. keys are unique and pre-partitioned by crc16 % 16383. If key contains {...} parts around it are excluded for partitioning (TODO: fix prefixes)
   */
  def redisHInput(initialHost: (String, Int), numPaprtitionsPerNode: Int = 1, useSlaves: Boolean = false, namespace: Int = 0,
                  scanCount: Int = 10000,
                  keyPattern: String = "*",
                  checkForKeyType: Boolean = false) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisHRDD(sc, hosts, namespace, scanCount, keyPattern, checkForKeyType)
  }

  /**
   *
   * @param jp Jedis pool containing host
   * @param useSlaves use slaves or not (not supported in current Jedis version)
   * @param numPaprtitionsPerNode umber of partitions per (master) node
   * @return list of nodes total counts of partitions per key range
   */
  def getSet(jp: JedisPool, useSlaves: Boolean, numPaprtitionsPerNode: Int) = {
    val s: scala.collection.mutable.Set[(String, Int, Int)] = scala.collection.mutable.Set()
    val resource = jp.getResource()
    val client = resource.getClient()
    var i = 0
    var master = false
    resource.info("replication").split("\n").foreach { repl =>
      if (repl.contains("role")) {
        if (repl.contains("master")) {
          master = true;
          (0 until numPaprtitionsPerNode).foreach { unused =>
            s.add((client.getHost(), client.getPort(), i))
            i += 1
          }
        }
        else {
          i = 0
          master = false
        }
      }
      if (useSlaves && master) {
        if (repl.startsWith("slave")) {

          val replSplit = repl.split("=");
          (0 until numPaprtitionsPerNode).foreach { unused =>
            s.add((replSplit(1).replace(",port", ""), replSplit(2).replace(",state", "").toInt, i))
            i += 1
          }
        }
      }
    }
    (s, i - 1)
  }

  /**
   * @param initialHost This is a tuple of any host and port in the cluster
   * @param numPaprtitionsPerNode Optional, number of partitions per (master) node, default 1
   * @param useSlaves Optional, reserved till when Jedis supports reading slaves in the Cluster, default 0
   * @param namespace Optional, reserved till when Redis supports namespaces in Cluster, default 0
   * @param scanCount Optional, allows to change number of keys returned in each scan iteration
   * @param keyPattern Optional, allows filtering by key on redis, before sending data to Spark
   * @param makePartitioner Optionally set if you want partitioner to be set (to join/co-group with similar Redis RDD without shuffle. Default true
   * @param valuePattern Optionally set filter to apply to set members
   * @return Pair RDD of  Key/Set members stored currently in Redis cluster. keys are unique and pre-partitioned by crc16 % 16383. If key contains {...} parts around it are excluded for partitioning (TODO: fix prefixes)
   */
  def redisSInput(initialHost: (String, Int),
                  numPaprtitionsPerNode: Int = 1,
                  useSlaves: Boolean = false,
                  namespace: Int = 0,
                  scanCount: Int = 10000,
                  keyPattern: String = "*",
                  makePartitioner: Boolean = true,
                  valuePattern: String = "*"
                   ) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisSRDD(sc, hosts, namespace, scanCount, keyPattern, makePartitioner, valuePattern)
  }

  /**
   * @param initialHost This is a tuple of any host and port in the cluster
   * @param numPaprtitionsPerNode Optional, number of partitions per (master) node, default 1
   * @param useSlaves Optional, reserved till when Jedis supports reading slaves in the Cluster, default 0
   * @param namespace Optional, reserved till when Redis supports namespaces in Cluster, default 0
   * @param scanCount Optional, allows to change number of keys returned in each scan iteration
   * @param keyPattern Optional, allows filtering by key on redis, before sending data to Spark
   * @param makePartitioner Optionally set if you want partitioner to be set (to join/co-group with similar Redis RDD without shuffle. Default true
   * @return Pair RDD of simple Key/Values stored currently in Redis cluster. keys are unique and pre-partitioned by crc16 % 16383. If key contains {...} parts around it are excluded for partitioning (TODO: fix prefixes)
   */
  def redisKInput(initialHost: (String, Int),
                  numPaprtitionsPerNode: Int = 1,
                  useSlaves: Boolean = false,
                  namespace: Int = 0,
                  scanCount: Int = 10000,
                  keyPattern: String = "*",
                  makePartitioner: Boolean = true
                   ) = {
    //For now only master nodes
    val jc = new JedisCluster(Set(new HostAndPort(initialHost._1, initialHost._2)), 5)
    val pools: util.Collection[JedisPool] = jc.getClusterNodes.values
    val hosts = pools.map(jp => getSet(jp, useSlaves, numPaprtitionsPerNode)).flatMap(x => x._1.zip(Seq.fill(x._1.size) {
      x._2
    })).map(s => (s._1._1, s._1._2, s._1._3, s._2)).toArray
    new RedisKRDD(sc, hosts, namespace, scanCount, keyPattern, makePartitioner)
  }

}