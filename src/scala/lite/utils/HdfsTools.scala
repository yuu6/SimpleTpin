package lite.utils

import _root_.java.math.BigDecimal
import _root_.java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import utils.Parameters

import scala.reflect.ClassTag

/**
  * Created by weiwenda on 2017/3/15.
  */
object HdfsTools {
  // 保存TPIN到HDFS
  def saveAsObjectFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                               verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd"): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(30).saveAsObjectFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(30).saveAsObjectFile(edgesFilePath)
  }

  // 保存TPIN到HDFS
  def saveAsTextFile[VD, ED](tpin: Graph[VD, ED], sparkContext: SparkContext,
                             verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd"): Unit = {

    checkDirExist(sparkContext, verticesFilePath)
    checkDirExist(sparkContext, edgesFilePath)
    // 对象方式保存顶点集
    tpin.vertices.repartition(1).saveAsTextFile(verticesFilePath)
    // 对象方式保存边集
    tpin.edges.repartition(1).saveAsTextFile(edgesFilePath)
  }

  def checkDirExist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    try {
      hdfs.delete(new Path(outpath), true)
    }
    catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  // 从HDFS获取TPIN
  def getFromObjectFile[VD:ClassTag, ED:ClassTag](sparkContext: SparkContext, verticesFilePath: String = "/tpin/object/vertices_wwd", edgesFilePath: String = "/tpin/object/edges_wwd")
  : Graph[VD, ED] = {
    // 对象方式获取顶点集
    val vertices = sparkContext.objectFile[(VertexId, VD)](verticesFilePath).repartition(30)
    // 对象方式获取边集
    val edges = sparkContext.objectFile[Edge[ED]](edgesFilePath).repartition(30)
    // 构建图
    Graph[VD,ED](vertices, edges)
  }

  def printGraph[VD, ED](graph: Graph[VD, ED]) = {
    graph.vertices.collect().foreach {
      println
    }
    graph.edges.collect().foreach { case edge => println(edge) }
  }

  def Exist(sc: SparkContext, outpath: String) = {
    val hdfs = FileSystem.get(new URI(Parameters.Home), sc.hadoopConfiguration)
    hdfs.exists(new Path(outpath))
  }
}
