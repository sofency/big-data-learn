package com.sofency.top.utils

import java.util.ResourceBundle

/**
 *
 * <p>Project: big-data-learn - ResourceUtils
 * <p>Powered by echo On 2023-11-02 15:57:57
 *
 * @author sofency [sofency@qq.com]
 * @version 1.0
 * @since 8
 */
object ResourceUtils {
  // 获取配置文件
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(key: String): String = {
    bundle.getString(key)
  }
}
