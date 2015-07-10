package com.javachen.grab.redis

import redis.clients.jedis.{JedisPoolConfig, JedisPool}

/**
 *
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-06-01 15:33.
 */
object RedisClient extends Serializable {

  // val redisHost = "localhost"
  // val redisPort = 6379
  // val redisTimeout = 3000

   val envConf = ConfigFactory.load()
   val redisHost = envConf.getString("redis.host")
   val redisPort = envConf.getInt("redis.port")
   val redisTimeout = envConf.getInt("redis.timeout")

  lazy val config = new JedisPoolConfig()
  config.setMaxIdle(50)
  config.setMaxIdle(10)
  lazy val pool = new JedisPool(config , redisHost, redisPort, redisTimeout)
  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run)

}
