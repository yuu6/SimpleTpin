package lite.main

import lite.entity.Pattern
import lite.entity.impl.{BasicEdgeAttr, BasicVertexAttr, InfluVertexAttr}
import lite.utils.HdfsTools
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
import utils.Parameters

/**
  * Created by weiwenda on 2018/5/3.
  */
class PatternMatching(@transient
                       sc:SparkContext,
                      @transient
                       session:SparkSession,var forceReComputePath: Boolean = false) extends Serializable{
  type Path = Seq[VertexId]
  type Paths = Seq[Seq[VertexId]]
  val hdfsDir: String = Parameters.Dir

  def _getOrComputePaths(tpin: Graph[BasicVertexAttr,BasicEdgeAttr], maxLength: Int, suffix: String) = {
    val path = s"${hdfsDir}/control_path${suffix}"
    if (!HdfsTools.Exist(sc, path) || forceReComputePath) {
      val initGraph = tpin
        .mapVertices { case (id, vattr) =>
          if (vattr.ishuman) Seq(Seq(id)) else Seq[Seq[VertexId]]()
        }
      def sendPaths(edge: EdgeContext[Paths,BasicEdgeAttr,Paths],
                    length: Int): Unit = {
        // 过滤掉仅含交易权重的边 以及非起点
        val satisfied = edge.srcAttr.filter(_.size==length).filter(!_.contains(edge.dstId))
        if (satisfied.size>0){
          // 向终点发送顶点路径集合
          edge.sendToDst(satisfied.map(_ ++ Seq(edge.dstId)))
        }
      }
      def reduceMsg(a: Paths, b: Paths): Paths = a ++ b
      val paths = ConstructInfn.getPathGeneric[Paths, BasicEdgeAttr](initGraph, sendPaths, reduceMsg,maxIteratons = maxLength,initLength=1).
        mapValues(e => e.filter(_.size > 1)).filter(e => e._2.size > 0).flatMap(_._2)
      HdfsTools.checkDirExist(sc, path)
      paths.repartition(30).saveAsObjectFile(path)
    }
    sc.objectFile[Seq[graphx.VertexId]](path).repartition(30)
  }
  //annotation of david:构建互锁关联交易模式（单向环和双向环），未考虑第三方企业的选择
  def matchPattern(graph: Graph[BasicVertexAttr, BasicEdgeAttr],length:Int,maxLength:Int=3): RDD[Pattern] = {
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = graph.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), 1)).cache()
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = _getOrComputePaths(graph,maxLength,suffix = "")
      .filter(_.size<=length)
      .map(e=>(e.last, e)).cache()

    //        println("模式2的前件路径个数：" + pathsWithLastVertex.count())
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges
      .keyBy(_._1._1).join(pathsWithLastVertex)
      .keyBy(_._2._1._1._2).join(pathsWithLastVertex)
      .map { case (dstid, ((srcid, (((srcid2, dstid2), cid), srcpath)), dstpath)) =>
        ((srcpath, dstpath), cid)
      }
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern = pathTuples
      .filter { case ((srcpath, dstpath), cid) =>
        srcpath(0) == dstpath(1) && srcpath(1) == dstpath(0) && srcpath.intersect(dstpath).size == 2
      }
      .map { case ((srcpath, dstpath), cid) =>
        val edges =
          (
            (Seq[Seq[Long]]() ++ srcpath.sliding(2) ++ dstpath.sliding(2)).filter(_.size == 2) ++
              Seq(Seq(srcpath.last, dstpath.last))
            ).map(list => (list(0), list(1)))
        val vertices = (srcpath.tail.tail ++ dstpath)
        val result = Pattern(2, edges, vertices)
        result.communityId = cid
        result.max_length = srcpath.size.max(dstpath.size)
        result
      }
    pattern
  }
}
object PatternMatching{
  def main(args: Array[String]) {
    val hdfsDir: String = Parameters.Dir
    val suffix = ""
    val ci = new ConstructInfn()
    val pm = new PatternMatching(ci.sc,ci.session)
    val tpin = ci.getOrReadTpin(Seq(s"${hdfsDir}/init_vertices", s"${hdfsDir}/init_edges"))
    pm.matchPattern(tpin,3)
  }
}
