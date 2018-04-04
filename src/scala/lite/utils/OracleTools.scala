package lite.utils

import _root_.java.text.DecimalFormat
import java.math.BigDecimal

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import lite.entity.impl.{InfluEdgeAttr, InfluVertexAttr, BasicEdgeAttr, BasicVertexAttr}
import lite.entity.{EdgeAttr, VertexAttr}
import utils.Parameters

/**
  * Oracle数据库存取工具
  *
  * Created by yzk on 2017/4/1.
  */
object OracleTools {

  //  sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  //  sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

  val options = Map("url"->Parameters.DataBaseURL,
    "driver"->Parameters.JDBCDriverString,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword)

  def saveNeighborInfo(rowRDD: DataFrame,session: SparkSession,dst:String="WWD_NEIGHBOR_INFO"): Unit = {
    import session.implicits._
    val jOptions = new JDBCOptions(options+(("dbtable",dst )))
    val conn = JdbcUtils.createConnectionFactory(jOptions)()
    if(JdbcUtils.tableExists(conn,jOptions)){
      JdbcUtils.truncateTable(conn,dst)
    }else{
      JdbcUtils.createTable(conn,rowRDD,jOptions)
    }
    JdbcUtils.saveTable(rowRDD,Option(rowRDD.schema), false, jOptions)
    conn.close()
  }
  /**
   *Author:weiwenda
   *Description:如果表存在清空，否则新建。
   *Date:16:19 2018/3/9
   */
  case class Vertex2DAH(vid: Long,nsrdzdah:Long)
  def saveVertexs(result:RDD[Vertex2DAH],session:SparkSession,dst:String="WWD_VERTEXS2DAH")={
    import session.implicits._
    val rowRDD = result.toDF()
    val jOptions = new JDBCOptions(options+(("dbtable",dst )))
    val conn = JdbcUtils.createConnectionFactory(jOptions)()
    if(JdbcUtils.tableExists(conn,jOptions)){
      JdbcUtils.truncateTable(conn,dst)
    }else{
      JdbcUtils.createTable(conn,rowRDD,jOptions)
    }
    JdbcUtils.saveTable(rowRDD,Option(rowRDD.schema), false, jOptions)
    conn.close()
  }
}
