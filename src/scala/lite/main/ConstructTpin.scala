package lite.main

import java.math.BigDecimal

import utils.Parameters
import lite.entity.impl.{BasicEdgeAttr, BasicVertexAttr}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._

/**
  * Created by weiwenda on 2018/4/4.
  */
object ConstructTpin {
  def main(args: Array[String]) {
    println("")
  }
  val options = Map("url"->Parameters.DataBaseURL,
    "driver"->Parameters.JDBCDriverString,
    "user" -> Parameters.DataBaseUserName,
    "password" -> Parameters.DataBaseUserPassword)
  def getFromOracleTable(sqlContext: SparkSession): Graph[BasicVertexAttr, BasicEdgeAttr] = {
    val dbstring = options
    import sqlContext.implicits._
    val gd_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_GD"))).load()
    val fddbr_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_FDDBR"))).load()
    val tzf_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_NSR_TZF"))).load()
    val trade_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_XFNSR_GFNSR"))).load()

    val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_GROUNDTRUTH"))).load()
    val xyjb = XYJB_DF.select("VERTEXID", "XYGL_XYJB_DM", "FZ", "WTBZ").rdd.
      map(row =>
      (row.getAs[BigDecimal]("VERTEXID").longValue(), (row.getAs[BigDecimal]("FZ").intValue(), row.getAs[String]("XYGL_XYJB_DM"), row.getAs[String]("WTBZ"))))

    //annotation of david:计算点表
    //unionAll不去重
    val GD_COMPANY_DF = gd_DF.
      filter($"JJXZ".startsWith("1") || $"JJXZ".startsWith("2") || $"JJXZ".startsWith("3")).
      selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL", "GDMC as NAME")
    val TZ_COMPANY_DF = tzf_DF.
      filter($"TZFXZ".startsWith("1") || $"TZFXZ".startsWith("2") || $"TZFXZ".startsWith("3")).
      selectExpr("ZJHM as TZ_ZJHM", "VERTEXID AS BTZ_VERTEXID", "TZBL", "TZFMC as NAME")
    val ZJHM_COMPANY_DF = GD_COMPANY_DF.unionAll(TZ_COMPANY_DF)
    val NSR_VERTEX = ZJHM_COMPANY_DF.selectExpr("TZ_ZJHM as ZJHM").except(fddbr_DF.select("ZJHM")).
      join(ZJHM_COMPANY_DF, $"ZJHM" === $"TZ_ZJHM").
      select("TZ_ZJHM", "NAME").
      rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("TZ_ZJHM"), false))

    val GD_ZZR_DF = gd_DF.filter($"JJXZ".startsWith("5") || $"JJXZ".startsWith("4"))
    val TZ_ZZR_DF = tzf_DF.filter($"TZFXZ".startsWith("5") || $"TZFXZ".startsWith("4"))

    val ZZR_VERTEX = fddbr_DF.selectExpr("ZJHM", "FDDBRMC as NAME").
      unionAll(GD_ZZR_DF.selectExpr("ZJHM", "GDMC as NAME")).
      unionAll(TZ_ZZR_DF.selectExpr("ZJHM", "TZFMC as NAME")).
      rdd.map(row => (row.getAs[String]("NAME"), row.getAs[String]("ZJHM"), true))

    val maxNsrID = fddbr_DF.agg(max("VERTEXID")).head().getDecimal(0).longValue()

    val ZZR_NSR_VERTEXID = ZZR_VERTEX.union(NSR_VERTEX).
      map { case (name, nsrsbh, ishuman) => (nsrsbh, BasicVertexAttr(name, nsrsbh, ishuman)) }.
      reduceByKey(BasicVertexAttr.combine).zipWithIndex().map { case ((nsrsbh, attr), index) => (index + maxNsrID, attr) }

    val ALL_VERTEX = ZZR_NSR_VERTEXID.
      union(fddbr_DF.
        select("VERTEXID", "NSRDZDAH", "NSRMC").
        rdd.map(row =>
        (row.getAs[BigDecimal]("VERTEXID").longValue(), BasicVertexAttr(row.getAs[String]("NSRMC"), row.getAs[BigDecimal]("NSRDZDAH").toString, false))
      )).
      leftOuterJoin(xyjb).
      map { case (vid, (vattr, opt_fz_dm)) =>
        if (!opt_fz_dm.isEmpty) {
          vattr.xyfz = opt_fz_dm.get._1
          vattr.xydj = opt_fz_dm.get._2
          if (opt_fz_dm.get._3.equals("Y")) vattr.wtbz = true;
        }
        (vid, vattr)
      }.
      persist(StorageLevel.MEMORY_AND_DISK)

    //annotation of david:计算边表

    //annotation of david:特别的，一个ZJHM可能匹配到多个纳税人
    val gd_cc = GD_COMPANY_DF.
      join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = BasicEdgeAttr(0.0, 0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }
    val tz_cc = TZ_COMPANY_DF.
      join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").
      select("VERTEXID", "BTZ_VERTEXID", "TZBL").
      rdd.map { case row =>
      val eattr = BasicEdgeAttr(0.0, row.getAs[BigDecimal](2).doubleValue(), 0.0, 0.0)
      ((row.getAs[BigDecimal](0).longValue(), row.getAs[BigDecimal](1).longValue()), eattr)
    }
    val gd_pc_cc = gd_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(GD_COMPANY_DF.join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.zjhm)).
      map { case (sbh1, ((dstid, gdbl), (srcid, attr))) =>
        val eattr = BasicEdgeAttr(0.0, 0.0, gdbl, 0.0)
        ((srcid, dstid), eattr)
      }
    val tz_pc_cc = tzf_DF.
      selectExpr("ZJHM", "VERTEXID", "TZBL").
      except(TZ_COMPANY_DF.join(fddbr_DF, $"TZ_ZJHM" === $"ZJHM").select("TZ_ZJHM", "BTZ_VERTEXID", "TZBL")).
      rdd.map(row => (row.getAs[String](0), (row.getAs[BigDecimal](1).longValue(), row.getAs[BigDecimal](2).doubleValue()))).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.zjhm)).
      map { case (sbh1, ((dstid, tzbl), (srcid, attr))) =>
        val eattr = BasicEdgeAttr(0.0, tzbl, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    val trade_cc = trade_DF.
      select("xf_VERTEXID", "gf_VERTEXID", "jybl", "je", "se", "sl").
      rdd.map { case row =>
      val eattr = BasicEdgeAttr(0.0, 0.0, 0.0, row.getAs[BigDecimal]("jybl").doubleValue())
      eattr.se = row.getAs[BigDecimal]("se").doubleValue()
      eattr.trade_je = row.getAs[BigDecimal]("je").doubleValue()
      eattr.taxrate = row.getAs[BigDecimal]("sl").doubleValue()
      ((row.getAs[BigDecimal]("gf_VERTEXID").longValue(),row.getAs[BigDecimal]("xf_VERTEXID").longValue()), eattr)
    }
    val fddb_pc = fddbr_DF.select("VERTEXID", "ZJHM").
      rdd.map(row => (row.getAs[String](1), row.getAs[BigDecimal](0).longValue())).
      join(ZZR_NSR_VERTEXID.keyBy(_._2.zjhm)).
      map { case (sbh1, (dstid, (srcid, attr))) =>
        val eattr = BasicEdgeAttr(1.0, 0.0, 0.0, 0.0)
        ((srcid, dstid), eattr)
      }
    // 合并控制关系边、投资关系边和交易关系边（类型为三元组逐项求和）,去除自环
    val ALL_EDGE = tz_cc.union(gd_cc).union(tz_pc_cc).union(gd_pc_cc).union(trade_cc).union(fddb_pc).
      reduceByKey(BasicEdgeAttr.combine).filter(edge => edge._1._1 != edge._1._2).
      map(edge => Edge(edge._1._1, edge._1._2, edge._2)).
      persist(StorageLevel.MEMORY_AND_DISK)
    //annotation of david:获取度大于0的顶点
    // Vertices with no edges are not returned in the resulting RDD.
    val degrees = Graph(ALL_VERTEX, ALL_EDGE).degrees.persist
    // 使用度大于0的顶点和边构建图
    val ALL_VERTEX_TMP = ALL_VERTEX.join(degrees).map(vertex => (vertex._1, vertex._2._1))
    Graph(ALL_VERTEX_TMP, ALL_EDGE).persist()
  }


}
