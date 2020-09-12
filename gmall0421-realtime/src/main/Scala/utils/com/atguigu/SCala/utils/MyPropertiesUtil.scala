package utils.com.atguigu.SCala.utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object MyPropertiesUtil {
    //读取配置文件的工具类

  //用Properties封装kafka键值对
  //用properties类读取config.properties配置信息 properties是map的孙子
  //properties是hashtable子类 hashtable是map子类
  //传一个propertiesname 到指定的破配置文件读取相关配置 返回一个properties
  def load(propertiesName:String): Properties = {
    val prop: Properties = new Properties()
    //加载输入中数据//加载某一个输入流中的数据
    prop.load(new InputStreamReader(
      ///通过currentThread获取当前线程
      Thread.currentThread()
        //通过线程找到类加载器，类加载器在哪就去哪获取
        .getContextClassLoader
        //获取文件名
        .getResourceAsStream(propertiesName)
      ,StandardCharsets.UTF_8

    ))
    prop
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }
}
