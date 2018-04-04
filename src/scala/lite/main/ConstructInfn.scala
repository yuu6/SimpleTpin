package lite.main

import lite.entity.impl.{BasicEdgeAttr, BasicVertexAttr, InfluVertexAttr, InfluEdgeAttr}
import lite.utils.HdfsTools
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory
import utils.Parameters

import scala.collection.{mutable, Seq}
import scala.reflect.ClassTag
/**
  * Author: weiwenda
  * 1.实现getGraph，内置强制计算和读缓存功能
  * 2.实现persist,将中间结果存储至HDFS
  *
  * Date: 下午8:08 2017/11/29
  */
class ConstructInfn(val forceReConstruct: Boolean = false) {
  @transient
  var sc: SparkContext = _
  @transient
  var session: SparkSession = _
  initialize()

  def initialize(): Unit = {
    session = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    sc = session.sparkContext
  }

  val log = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  val hdfsDir: String = Parameters.Dir

  def showDimension[VD, ED](graph: Graph[VD, ED], title: String): Unit = {
    log.info(s"\r${title}=>edges:${graph.edges.count()},vertices:${graph.vertices.count()}")
  }

  def getOrReadTpin(paths: Seq[String]) = {
    if (!HdfsTools.Exist(sc, paths(0)) || !HdfsTools.Exist(sc, paths(1)) || forceReConstruct) {
      val tpin = ConstructTpin.getFromOracleTable(session).persist()
      showDimension(tpin, "从Oracle读入")
      ConstructInfn.persist(tpin, paths,sc)
    } else {
      HdfsTools.getFromObjectFile[BasicVertexAttr, BasicEdgeAttr](sc, paths(0), paths(1)).persist()
    }
  }

  /**
    * Author: weiwenda
    * Description: 从Oracle读入，添加互锁边，并保存至HDFS
    * Date: 下午4:22 2017/11/28
    */
  def getGraph(sc: SparkContext, session: SparkSession) = {
    val paths = Seq(s"${hdfsDir}/addil_vertices", s"${hdfsDir}/addil_edges")
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc, paths(0)) || !HdfsTools.Exist(sc, paths(1)) || forceReConstruct) {
      val tpin = getOrReadTpin(Seq(s"${hdfsDir}/init_vertices", s"${hdfsDir}/init_edges"))
      //annotation of david:这里的互锁边为董事会互锁边
      val tpinWithIL = ConstructInfn.addIL(tpin, weight = 0.0).persist()
      val tpinOnlyCompany = ConstructInfn.transform(tpinWithIL)
      ConstructInfn.persist(tpinOnlyCompany, paths,sc)
    } else {
      HdfsTools.getFromObjectFile[InfluVertexAttr, InfluEdgeAttr](sc, paths(0), paths(1))
    }
  }
}

object ConstructInfn {
  def persist[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], outputPaths: Seq[String],sc:SparkContext): Graph[VD, ED] = {
    HdfsTools.saveAsObjectFile(graph, sc, outputPaths(0), outputPaths(1))
    HdfsTools.getFromObjectFile[VD, ED](sc, outputPaths(0), outputPaths(1))
  }
  //annotation of david:将传统的tpin转换为影响力初始网络
  def transform(tpinWithIL: Graph[BasicVertexAttr, BasicEdgeAttr]) = {
    val toReturn = tpinWithIL.subgraph(vpred = (vid, attr) => !attr.ishuman).
      mapEdges(map = { edge =>
        val eattr = InfluEdgeAttr()
        eattr.jy_bl = edge.attr.w_trade
        eattr.kg_bl = edge.attr.w_gd
        eattr.tz_bl = edge.attr.w_tz
        eattr.il_bl = edge.attr.w_IL
        eattr
      }).
      mapVertices(map = { (vid, vattr) =>
        val newattr = InfluVertexAttr(vattr.zjhm, vattr.name)
        newattr.xydj = vattr.xydj
        newattr.xyfz = vattr.xyfz
        newattr.wtbz = vattr.wtbz
        newattr
      })
    toReturn
  }

  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]

  def findFrequence(d: Seq[(VertexId, mutable.Map[Long, Double])]): Seq[((Long, Long), Double)] = {
    val frequencies = d
    val result =
      for (i <- 0 until frequencies.length) yield
        for (j <- i + 1 until frequencies.length) yield {
          val (vid1, list1) = frequencies(i)
          val (vid2, list2) = frequencies(j)
          val intersect = list1.keySet.intersect(list2.keySet)
          var weight = 0D
          for (key <- intersect){
            weight += list1.getOrElse(key,0.0).min(list2.getOrElse(key,0.0))
          }
          if (weight > 0)
            Option(Iterable(((vid1, vid2), weight), ((vid2, vid1), weight)))
          else
            Option.empty
        }
    result.flatten.filter(!_.isEmpty).map(_.get).flatten
  }
  /**
   *Author:weiwenda
   *Description:添加董事会互锁边,默认重叠度要求为1
   *Date:13:00 2018/4/4
   */
  def addIL(graph: Graph[BasicVertexAttr, BasicEdgeAttr], weight: Double) = {
    //annotation of david:仅从单条路径发送消息，造成了算法的不一致
    // 含义：每个人所控制及间接控制企业的列表
    val initialGraph = graph.
      mapVertices { case (id, vattr) =>
        if (vattr.ishuman) Seq(Seq((id, 1D))) else Seq[Seq[(VertexId, Double)]]()
      }.
      subgraph(epred = triplet =>
        triplet.attr.isAntecedent(weight)).
      mapEdges(edge => Seq(edge.attr.w_control, edge.attr.w_gd, edge.attr.w_tz).max).cache()
    //annotation of david:此处无法使用反向获取路径，因为要求源点必须是人
    //annotation of david:存在平行路径的问题(resolved)
    def sendPaths(edge: EdgeContext[Paths, Double, Paths], length: Int): Unit = {
      // 过滤掉仅含交易权重的边 以及非起点
      val satisfied = edge.srcAttr.filter(_.size == length).filter(!_.map(_._1).contains(edge.dstId))
      if (satisfied.size > 0) {
        // 向终点发送顶点路径集合
        edge.sendToDst(satisfied.map(_ ++ Seq((edge.dstId, edge.attr))))
      }
    }
    def reduceMsg(a: Paths, b: Paths): Paths = a ++ b

    val messagesOfControls = getPathGeneric(_removeIsolate(initialGraph),sendPaths,reduceMsg,maxIteratons=3).mapValues { lists =>
      val result = mutable.HashMap[Long, Double]()
      lists.filter(_.size > 1).foreach{ case list =>
        val influ = list.map(_._2).min
        result.update(list.head._1,result.getOrElse(list.head._1,influ).max(influ))
      }
      result
    }.filter(_._2.size > 0)
    val messagesOffDirection = messagesOfControls
      .flatMap { case (vid, controllerList) =>
        controllerList.map(controller =>
          (controller._1, (vid, controllerList))
        )
      }.groupByKey().map { case (vid, ite) => (vid, ite.toSeq) }
    val newILEdges = messagesOffDirection
      .flatMap { case (dstid, list) => findFrequence(list) }
      .distinct
      .map { case ((src, dst), weight) =>
        val edgeAttr = BasicEdgeAttr()
        edgeAttr.is_IL = true
        edgeAttr.w_IL = weight
        Edge(src, dst, edgeAttr)
      }
    val newEdges = graph.edges.union(newILEdges).
      map(e => ((e.srcId, e.dstId), e.attr)).
      reduceByKey(BasicEdgeAttr.combine).
      map(e => Edge(e._1._1, e._1._2, e._2))
    Graph(graph.vertices, newEdges)
  }
  /**
    *Author:weiwenda
    *Description:去掉网络中度为0的点
    *Date:10:12 2018/3/29
    */
  def _removeIsolate[VD:ClassTag,ED:ClassTag](fullTpin:Graph[VD,ED]):Graph[VD,ED] ={
    val degreesRDD = fullTpin.degrees.cache()
    var preproccessedGraph = fullTpin.
      outerJoinVertices(degreesRDD)((vid, vattr, degreesVar) => (vattr, degreesVar.getOrElse(0))).
      subgraph(vpred = {
        case (vid, (vattr, degreesVar)) =>
          degreesVar > 0
      }).
      mapVertices{case (vid,(attr,degree))=>attr}
    preproccessedGraph
  }
  /**
    * Author:weiwenda
    * Description:泛型版收集所有长度initlength到maxIteration的路径
    * graph为输入图
    * sendMsg用于带入每次所发消息
    * reduceMsg用于点层聚合消息，一般为_++_
    * maxIteratons 与 initLength共同决定迭代终点，最终的路径Seq长度为maxIteratons+1,最短的路径Seq长度为initLength
    * Date:17:25 2017/12/21
    */
  def getPathGeneric[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                 sendMsg: (EdgeContext[VD, ED, VD], Int) => Unit,
                                                 reduceMsg: (VD, VD) => VD,
                                                 maxIteratons: Int = Int.MaxValue, initLength: Int = 1) = {
    // 发送路径
    var preproccessedGraph = graph.cache()
    var i = initLength
    var messages = preproccessedGraph.aggregateMessages[VD](sendMsg(_, i), reduceMsg)
    var activeMessages = messages.count()
    var prevG: Graph[VD, ED] = null
    while (activeMessages > 0 && i <= maxIteratons) {
      prevG = preproccessedGraph
      preproccessedGraph = preproccessedGraph.joinVertices[VD](messages)((id, vd, path) => reduceMsg(vd, path)).cache()
      print("iterator " + i + " finished! ")
      i += 1
      if (i <= maxIteratons) {
        val oldMessages = messages
        messages = preproccessedGraph.aggregateMessages[VD](sendMsg(_, i), reduceMsg).cache()
        try {
          activeMessages = messages.count()
        } catch {
          case ex: Exception =>
            println("又发生异常了")
        }
        oldMessages.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
      }
    }
    //         printGraph[Paths,Int](preproccessedGraph)
    preproccessedGraph.vertices
  }
}

