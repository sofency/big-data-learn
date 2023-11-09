package com.sofency.top.utils

import com.sofency.top.constants.ConfigConstant
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  //创建连接池对象
  private var jedisPool: JedisPool = null

  /**
   * 通过连接池对象获取jedis
   * @return
   */
  def getJedisFromPool(): Jedis = {
    if (jedisPool == null) {
      //创建连接池对象
      val host = ResourceUtils(ConfigConstant.REDIS_HOST)
      val port = ResourceUtils(ConfigConstant.REDIS_PORT)
      //连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
}