package com.sofency.top.constants

/**
 *
 * <p>Project: big-data-learn - ConfigConstant
 * <p>Powered by echo On 2023-11-02 16:15:20
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object ConfigConstant {
  val KAFKA_BOOT_SERVER: String = "kafka_boot_server"

  val KEY_SERIALIZER: String = "key_serializer"

  val VALUE_SERIALIZER: String = "value_serializer"

  val KEY_DESERIALIZER: String = "key_deserializer"

  val VALUE_DESERIALIZER: String = "value_deserializer"

  val ENABLE_AUTO_COMMIT: String = "enable_auto_commit"

  val AUTO_COMMIT_INTERVAL_MS: String = "auto_commit_interval_ms"

  val AUTO_OFFSET_RESET: String = "auto_offset_reset"

  val ACK: String = "ack"

  val ENABLE_IDEMPOTENCE: String = "enable_idempotence"

  val REDIS_HOST: String = "redis_host"

  val REDIS_PORT: String = "redis_port"
}
