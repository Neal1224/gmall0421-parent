package utils.com.atguigu.SCala.utils

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{DocumentResult, Get, Index, Search, SearchResult}


//操作ES工具类
object MyESUtil {
//一般不用以下方法 这个方法还得去es网站测试是否能添加才能拿到这里来用
  //|    "id":2,
  //        |    "name":"湄公河行动",
  //        |    "doubanScore":8.0,
  //        |    "actorList":[
  //        |    {"id":3,"name":"张涵予"}
  //        |    ]
  //        |  }
  //优化思路，以上是个文档信息通过封装的方式，封装到类中，把文档作为对象
  //方式2在方式1下面



//方式1
//  //声明一个JestClientFactory
//  private var factory:JestClientFactory  = null
//
//  //获取JestClient
//  def getClient(): JestClient ={
//    if(factory == null){
//      build();
//    }
//    factory.getObject
//  }
//  //创建JestClientFactory
//  def build():Unit = {
//    factory = new JestClientFactory
//    //给当前工程设置配置（并发 超时时间）
//    factory.setHttpClientConfig(
//      //HttpClientConfig
//      new HttpClientConfig.Builder("http://hadoop202：9200")
//        //最大连接数
//        .maxTotalConnection(10)
//        //是否使用多线程
//        .multiThreaded(true)
//        //10S（10000毫秒）钟连接不上报timeout
//        .connTimeout(10000)
//        .readTimeout(1000)
//        .build()
//    )
//  }
//  //向ES的Index中插入Document
//
//  /*
//    |{
//        |    "id":2,
//        |    "name":"湄公河行动",
//        |    "doubanScore":8.0,
//        |    "actorList":[
//        |    {"id":3,"name":"张涵予"}
//        |    ]
//        |  }
//
//   */
//  /*要想执行putindex操作
//    1.指定好source
//    2.通过builder指定index的type，id
//    3.传递给execute客户端方法
//   */
//  def putIndex():Unit = {
//    //获取客户端连接
//    val jestClient: JestClient = getClient()
//
//    var source =
//      """
//        {
//        |           "id":2,
//        |           "name":"湄公河行动",
//        |           "doubanScore":8.0,
//        |           "actorList":[
//        |          {"id":3,"name":"张涵予"}
//        |            ]
//        |          }
//        |""".stripMargin
//    //
//    val index: Index = new Index.Builder(source)
//      .index("movie_index_5")
//      .`type`("movie")
//      .id("1")
//      .build()
//    //
//    jestClient.execute(index)
//
//    //关闭连接
//    jestClient.close()
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    putIndex()
//  }


  //方式2
  //封装样例类存放文档信息
  //关键点：能存放样例类，封装文档就行
    //向ES的Index中插入文档



  //{
    //        |           "id":2,
    //        |           "name":"湄公河行动",
    //        |           "doubanScore":8.0,
    //        |           "actorList":[
    //        |          {"id":3,"name":"张涵予"}
    //        |            ]
    //        |          }
    //        |""".stripMargin

  //声明一个JestClientFactory
  private var factory: JestClientFactory = null

  //获取JestClient
  def getClient(): JestClient ={
    if(factory == null){
      build();
    }
    factory.getObject
  }

  //创建JestClientFactory
  def build():Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder("http://hadoop202:9200")
        .maxTotalConnection(10)
        .connTimeout(10000)
        .readTimeout(1000)
        .multiThreaded(true)
        .build()
    )
  }


  def putIndex():Unit={
    //建立连接
    val jestClient: JestClient = getClient()
    //底层会将source转换为json格式字符串进行操作
    val actorList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val actorMap: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    actorMap.put("id",10)
    actorMap.put("name","韦小宝")
    actorList.add(actorMap)
    val index: Index = new Index.Builder(
      Movie(
        11,"鹿鼎记",8.5,actorList
      )
    )
      .index("movie_index_6").`type`("movie").id("2")
      .build()



    //执行操作
    jestClient.execute(index)
    //关闭连接
    jestClient.close()
  }



  //根据文档id从ES索引中查询数据
  //GET /movie_index_5/movie/1
  def queryIndexById(): Unit ={
    val jestClient = getClient()
    val get: Get = new Get.Builder("movie_index_6","1").build()
    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)
    jestClient.close()
  }

  //根据查询条件从Index中查询文档信息
  def queryIndex(): Unit ={
    val jestClient = getClient()
    var queryStr =
      """
        |{
        |  "query": {
        |    "bool": {
        |       "must": [
        |        {"match": {
        |          "name": "red"
        |        }}
        |      ],
        |      "filter": [
        |        {"term": { "actorList.name.keyword": "zhang han yu"}}
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 20,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
      """.stripMargin
    val search = new Search.Builder(queryStr)
      .addIndex("movie_index").build()
    val result: SearchResult = jestClient.execute(search)
    //因为ES是Java语言编写，所以在封装集合类型的时候，需要使用Java的集合类型
    val resList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String,Any]])
    //为了操作方便，将Java类型集合转换为scala类型
    import scala.collection.JavaConverters._
    val newList = resList.asScala.map(_.source).toList
    println(newList.mkString("\n"))
    jestClient.close()
  }
  def main(args: Array[String]): Unit = {
    //putIndex()
    queryIndex
  }



}
//样例类封装
case class Movie(
                  id:Long,
                  name:String,
                  doubanScore:Double,
                  actorList:util.List[util.Map[String,Any]]

                )
