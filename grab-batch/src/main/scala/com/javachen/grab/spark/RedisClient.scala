package com.javachen.grab.spark

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
 *
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-06-01 15:33.
 */
object RedisClient extends Serializable{
  val redisHost = "127.0.0.1"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

}
