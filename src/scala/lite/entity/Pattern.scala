package lite.entity

import lite.entity.Pattern.NodePair
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

/**
  * Created by david on 6/29/16.
  */
class Pattern(val pid: Int, val edges: Seq[NodePair], val vertices: Seq[VertexId]) extends Serializable {
    var communityId: Long = 0
    var max_length:Int = 0

    //annotation of david:交易边的源点和终点
    val src_id: Long = edges.last._1
    val dst_id: Long = edges.last._2

    override def toString = s"Pattern(Edges{${edges.mkString(",")}}, Vertex $vertices)"

    def canEqual(other: Any): Boolean = other.isInstanceOf[Pattern]

    override def equals(other: Any): Boolean = other match {
        case that: Pattern =>
            (that canEqual this) &&
                src_id == that.src_id &&
                dst_id == that.dst_id
        case _ => false
    }

    override def hashCode(): Int = {
        val state = Seq(src_id, dst_id)
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
}

object Pattern {
    type NodePair = (VertexId, VertexId)

    // getFromHiveTable(hiveContext,2,3)
    def getFromHiveTable(hiveContext: HiveContext,month:Int,patternType: Int,vertexTableName:String="tpin_pattern_month_wwd") = {
        //hiveContext.sql("select ychz from tpin_zhibiao_wwd where month = 2 and id = 3559003 ").rdd.map(row=> row.getAs[Int](0)).collect
        //    hiveContext.sql("select ychz from tpin_zhibiao_wwd where month = 2 and id = 1867165 ").rdd.map(row=> row.getAs[Int](0)).collect
        //    hiveContext.sql("select src_flag from tpin_pattern_month_wwd where month = 2 and src_id = 3559003 ").rdd.map(row=> row.getAs[Int](0)).collect
        //hiveContext.sql("select count(*) from tpin_pattern_month_wwd ").rdd.map(row=> row.getAs[Long](0)).collect
        val patternInfo = hiveContext.sql("SELECT src_id,dst_id FROM "+vertexTableName+" where month = " + month + " and type = " + patternType).rdd

        //annotation of david:不去重以保证每个模式均不被覆盖
        val patterns = patternInfo.map(row => (row.getAs[Long]("src_id"), row.getAs[Long]("dst_id")))
                //.distinct()
            .map { case e =>
            val pattern = Pattern(patternType, Seq((e._1, e._2)), Seq(e._1, e._2))
            pattern
        }
        patterns
    }

    def CheckIntersect(pattern1: RDD[Pattern], pattern2: RDD[Pattern]) = {

        val intersect1 = pattern1.map(e => (e.edges.last, 1)).join(pattern2.distinct().map(e => (e.edges.last, 1))).count
        val intersect2 = pattern1.distinct().map(e => (e.edges.last, 1)).join(pattern2.distinct().map(e => (e.edges.last, 1))).count
        (intersect1,intersect2)
    }

    def apply(pid: Int, edges: Seq[NodePair], vertices: Seq[VertexId]) = {
        new Pattern(pid, edges, vertices)
    }

    // 保存关联交易模式（单向环和双向环）到Hive ，不带异常信息和月份信息 //tpin_pattern_wwd
    def saveAsHiveTable(hiveContext: HiveContext, pattern: RDD[Pattern],vertexTableName:String="tpin_pattern_wwd"): Unit = {

        hiveContext.sql("truncate table " + vertexTableName)
        // 构建边集字段（边间分号分隔，边内起点终点逗号分隔）
        def constructEdgesString(edges: Seq[(VertexId, VertexId)]): String = {
            var toReturn = ""
            for ((src, dst) <- edges) {
                toReturn += src + "," + dst + ";"
            }
            toReturn.substring(0, toReturn.length - 1)
        }
        // 构建顶点集字段（逗号分隔）
        def constructVerticesString(vertices: Seq[VertexId]): String = {
            var toRetrun = ""
            for (vid <- vertices) {
                toRetrun += vid + ","
            }
            toRetrun.substring(0, toRetrun.length - 1)
        }
        // 写入模式表
        hiveContext.createDataFrame(pattern.zipWithIndex.
            map(pattern1WithId => Row(pattern1WithId._2, pattern1WithId._1.pid, constructVerticesString(pattern1WithId._1.vertices), constructEdgesString(pattern1WithId._1.edges), pattern1WithId._1.communityId)),
            StructType(StructField("id", LongType) :: StructField("type", IntegerType) :: StructField("vertices", StringType) :: StructField("edges", StringType) :: StructField("community_id", LongType) :: Nil))
            .write.insertInto(vertexTableName)
    }

}
















