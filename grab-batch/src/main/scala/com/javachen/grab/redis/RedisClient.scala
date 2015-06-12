package com.javachen.grab.redis

import _root_.redis.clients.jedis.JedisPool
import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

/**
 *
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-06-01 15:33.
 */
object RedisClient extends Serializable {
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  val envConf = ConfigFactory.load()
  val redisHost = envConf.getString("redis.host")
  val redisPort = envConf.getInt("redis.port")
  val redisTimeout = envConf.getInt("redis.timeout")
  sys.addShutdownHook(hook.run)

}
