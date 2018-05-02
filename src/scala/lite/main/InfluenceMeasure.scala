package lite.main

import java.math.BigDecimal

import lite.entity.EdgeAttr
import lite.entity.impl.{InfluEdgeAttr, InfluVertexAttr}
import lite.utils.{OracleTools, HdfsTools}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.{Correlation => CorrelationStat}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import utils.Parameters

import scala.collection.Seq
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
  * Author: weiwenda
  * Description: 进行影响关系度量
  * Date: 下午9:00 2017/11/29
  */
case class FuzzEdgeAttr(val influ: Double) extends EdgeAttr

case class Correlation(vid: Long, wtbz: Integer, xyfz: Integer, nais: Double,
                       nwais0: Double, nwais1: Double, nwais2: Double, nwais3: Double, nwais4: Double, nwais5: Double,
                       nwais6: Double, nwais7: Double, nwais8: Double, nwais9: Double, nwais10: Double, ycsl: Double, dfsl: Integer, qysl: Integer)

case class VNFeature(var vid: Long, var pte: Double, var wtbz: Integer, var nwapte: Double,
                     var n1: Integer, var n2: Integer, var n3: Integer, var n4: Integer, var n5: Integer,
                     var n6: Integer, var n7: Integer, var n8: Integer, var n9: Integer, var n10: Integer,
                     var n11: Integer, var cluster: Double)

class InfluenceMeasure(ignoreIL: Boolean = false, forceReAdjust: Boolean = false,
                       var forceReComputePath: Boolean = false
                      ) {
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

  val hdfsDir: String = Parameters.Dir
  type Path = Seq[(VertexId, Double)]
  type Paths = Seq[Seq[(VertexId, Double)]]
  var rules: HashMap[String, Map[String, Double]] = _
  val membership = Range.Double(0, 1.01, 0.01).map { x =>
    val a = 4
    val b = -4
    val c = 0.2
    if (x - c > 0)
      (x, 1 / (1 + Math.pow(a * (x - c), b)))
    else
      (x, 0D)
  }

  def _getMap(seq1: Array[(Double, Int)], amount: Long): Map[String, Double] = {
    Range.Double(0, 1.01, 0.02).map(e => (e, seq1.filter(_._1 <= e).map(_._2).sum / amount.toDouble)).toMap.
      map { case (key, value) =>
        (key.formatted("%.3f"), value)
      }
  }

  def _getRules(graph: Graph[InfluVertexAttr, InfluEdgeAttr]) = {
    val result = HashMap[String, Map[String, Double]]()
    var seq1 = Array[(Double, Int)]()
    var amount: Long = 0
    seq1 = graph.edges.filter(e => e.attr.il_bl > 0).map(e => (Math.ceil(e.attr.il_bl / 0.02) * 0.02, 1)).reduceByKey(_ + _).sortByKey().collect()
    amount = graph.edges.filter(e => e.attr.il_bl > 0).count()
    result.put("il", _getMap(seq1, amount))
    seq1 = graph.edges.filter(e => e.attr.tz_bl > 0).map(e => (Math.ceil(e.attr.tz_bl / 0.02) * 0.02, 1)).reduceByKey(_ + _).sortByKey().collect()
    amount = graph.edges.filter(e => e.attr.tz_bl > 0).count()
    result.put("tz", _getMap(seq1, amount))
    seq1 = graph.edges.filter(e => e.attr.kg_bl > 0).map(e => (Math.ceil(e.attr.kg_bl / 0.02) * 0.02, 1)).reduceByKey(_ + _).sortByKey().collect()
    amount = graph.edges.filter(e => e.attr.kg_bl > 0).count()
    result.put("kg", _getMap(seq1, amount))
    seq1 = graph.edges.filter(e => e.attr.jy_bl > 0).map(e => (Math.ceil(e.attr.jy_bl / 0.02) * 0.02, 1)).reduceByKey(_ + _).sortByKey().collect()
    amount = graph.edges.filter(e => e.attr.jy_bl > 0).count()
    result.put("jy", _getMap(seq1, amount))
    result
  }

  def _newRule(s: String, bl: Double) = {
    rules.get(s).get.get((Math.ceil(bl / 0.02) * 0.02).min(1D).formatted("%.3f")).get
  }

  def _computeFuzzScore(il_bl: Double, tz_bl: Double, kg_bl: Double, jy_bl: Double) = {
    //annotation of david:计算最大的规则前件隶属度
    val upper = Seq(_newRule("il", il_bl), _newRule("tz", tz_bl), _newRule("kg", kg_bl), _newRule("jy", jy_bl)).max
    val index = membership.indexWhere { case (key, value) => value >= upper }
    val newindex = if (index == -1) membership.size - 1 else index
    membership(newindex)._1
  }

  /**
    * Author:weiwenda
    * Description:进行直接影响关系度量
    * Date:20:00 2018/3/28
    */
  def _getOrComputeInflu(tpin: Graph[InfluVertexAttr, InfluEdgeAttr], suffix: String) = {
    val paths = Seq(s"${hdfsDir}/fuzz_vertices${suffix}", s"${hdfsDir}/fuzz_edges${suffix}")
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc, paths(0)) || !HdfsTools.Exist(sc, paths(1)) || forceReComputePath) {
      rules = _getRules(tpin)
      val inferenced = tpin.mapTriplets { case triplet =>
        //annotation of david:bel为概率下限，pl为概率上限
        //                      当ignoreIL为true时，无视il_bl
        val bel = _computeFuzzScore(if (ignoreIL) 0 else triplet.attr.il_bl, triplet.attr.tz_bl, triplet.attr.kg_bl, triplet.attr.jy_bl)
        val attr = FuzzEdgeAttr(bel)
        attr
      }
      //annotation of david:此处限制了出度
      val simplifiedGraph = _simpleGraph(inferenced, (vid_attr: (Long, FuzzEdgeAttr)) => vid_attr._2.influ, 5)
      ConstructInfn.persist(simplifiedGraph, paths,sc)
    } else {
      HdfsTools.getFromObjectFile[InfluVertexAttr, FuzzEdgeAttr](sc, paths(0), paths(1))
    }
  }

  def _getOrComputePaths(simplifiedGraph: Graph[InfluVertexAttr, FuzzEdgeAttr], maxLength: Int, suffix: String) = {
    val path = s"${hdfsDir}/fuzz_path${suffix}"
    //annotation of david:forceReConstruct=true表示强制重新构建原始TPIN,默认不强制
    if (!HdfsTools.Exist(sc, path) || forceReComputePath) {
      //annotation of david:企业对自身的bel和pl均为1
      val initGraph = simplifiedGraph.mapVertices { case (vid, nsrdzdah) => Seq(Seq((vid, 1.0))) }

      def sendPaths(edge: EdgeContext[Paths, FuzzEdgeAttr, Paths],
                    length: Int): Unit = {
        val satisfied = edge.dstAttr.filter(e => e.size == length).filter(e => !e.map(_._1).contains(edge.srcId))
        if (satisfied.size > 0) {
          // 向终点发送顶点路径集合，每个经过节点的id,sbh,当前经过边的bel,pl,原始4维权重
          edge.sendToSrc(satisfied.map(Seq((edge.srcId, edge.attr.influ)) ++ _))
        }
      }
      def reduceMsg(a: Paths, b: Paths): Paths = a ++ b
      val paths = ConstructInfn.getPathGeneric[Paths, FuzzEdgeAttr](initGraph, sendPaths, reduceMsg, maxIteratons = maxLength, initLength = 1).
        mapValues(e => e.filter(_.size > 1)).filter(e => e._2.size > 0)
      HdfsTools.checkDirExist(sc, path)
      paths.repartition(30).saveAsObjectFile(path)
    }
    sc.objectFile[(Long, Seq[Seq[(graphx.VertexId, Double)]])](path).repartition(30)
  }

  /**
    * Author:weiwenda
    * Description:过滤影响权重过小的边
    * Date:21:01 2018/3/28
    */
  def _influenceInTotal(influenceGraph: Graph[InfluVertexAttr, Seq[Double]]) = {
    val toReturn = influenceGraph.subgraph(epred = triplet => triplet.attr(0) > 0.01)
    toReturn
  }

  /**
    * Author:weiwenda
    * Description:Frank t-norm family
    * Date:15:18 2018/3/29
    */
  def _combineInfluence(x: (VertexId, Double), y: (VertexId, Double), lambda: Double): (VertexId, Double) = {
    var pTrust = 0D
    val a = x._2
    val b = y._2
    if (lambda == 0) pTrust = a.min(b)
    else if (lambda == 1) pTrust = a * b
    else if (lambda == 100000) pTrust = (a + b - 1).max(0.0)
    else pTrust = Math.log(1 + (((Math.pow(lambda, a) - 1) * ((Math.pow(lambda, b) - 1))) / (lambda - 1))) / Math.log(lambda)
    (y._1, pTrust)
  }

  //annotation of david:使用三角范式计算路径上的影响值（包含参照影响逻辑和基础影响逻辑）
  protected def _influenceOnPath(paths: RDD[(VertexId, Paths)], sqlContext: SparkSession,
                                 pathLength: Int) = {
    val lambdaList = Seq[Double](0, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 10000, 100000)
    val influences = paths.
      map { case (vid, vattr) =>
        val influenceSinglePath = vattr.
          filter(_.size <= pathLength + 1).
          map { path =>
            val dst = path.last._1
            val Aid = for {
              lambdaLocal <- lambdaList
              res = path.reduceLeft((a, b) => _combineInfluence(a, b, lambdaLocal))
            } yield res._2
            (dst, Aid)
          }
        (vid, influenceSinglePath)
      }.
      flatMap { case (vid, list) =>
        list.map { case (dstid, influ) => ((vid, dstid), influ) }
      }.
      reduceByKey((a, b) => if (a(0) > b(0)) a else b).
      map { case ((vid, dstid), influ) => Edge(vid, dstid, influ) }
    influences
  }

  /**
    * Author:weiwenda
    * Description:泛型版的选择TopN邻居
    * belAndPl为需要简化的图
    * getWeight是从边属性中选择比较项的带入函数
    * selectTopN为所要选择的N
    * Date:17:22 2017/12/21
    */
  def _simpleGraph[VD: ClassTag, ED: ClassTag](belAndPl: Graph[VD, ED], getWeight: ((Long, ED)) => Double, selectTopN: Int = 20): Graph[VD, ED] = {
    def sendMessage(edge: EdgeContext[VD, ED, Seq[(VertexId, ED)]]): Unit = {
      edge.sendToSrc(Seq((edge.dstId, edge.attr)))
    }

    val messages = belAndPl.aggregateMessages[Seq[(VertexId, ED)]](sendMessage(_), _ ++ _).cache()

    val filtered_edges = messages.map { case (vid, edgelist) =>
      (vid, edgelist.sortBy[Double](getWeight)(Ordering[Double].reverse).slice(0, selectTopN))
    }.flatMap { case (vid, edgelist) => edgelist.map(e => Edge(vid, e._1, e._2)) }


    Graph[VD, ED](belAndPl.vertices, filtered_edges).persist()
  }

  /**
    * Author: weiwenda
    * Description: 从Oracle数据库中读取wtbz
    * Date: 下午4:48 2017/11/29
    */
  def _getScore() = {
    val sqlContext = session
    val dbstring = OracleTools.options
    val XYJB_DF = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_SELF_INFO"))).load()
    val VERTEXS2DAH = sqlContext.read.format("jdbc").options(dbstring + (("dbtable", "WWD_VERTEXS2DAH"))).load()
    val xyjb = XYJB_DF.join(VERTEXS2DAH, "NSRDZDAH").select("vid", "SCORE", "WTBZ").rdd.
      map(row =>
        if (row.getAs[BigDecimal]("WTBZ").intValue() == 1)
          (row.getAs[BigDecimal]("vid").longValue(), (row.getAs[BigDecimal]("SCORE").doubleValue(), true))
        else
          (row.getAs[BigDecimal]("vid").longValue(), (row.getAs[BigDecimal]("SCORE").doubleValue(), false))
      )
    xyjb
  }

  /**
    * Author: weiwenda
    * Description: 进行影响关系度量，并滤除影响权重过小的边
    * Date: 下午6:29 2018/4/1
    */
  def computeInfluencePro(tpin: Graph[InfluVertexAttr, InfluEdgeAttr], suffix: String = "", pathLength: Int = 4, maxLength: Int = 4): Graph[InfluVertexAttr, Seq[Double]] = {
    val simplifiedGraph = _getOrComputeInflu(tpin, suffix)
    val paths = _getOrComputePaths(simplifiedGraph, maxLength, suffix)
    //annotation of david:使用第一种三角范式
    val influenceEdge = _influenceOnPath(paths, session, pathLength)
    val influenceGraph = Graph(simplifiedGraph.vertices, influenceEdge).persist()

    //annotation of david:滤除影响力过小的边
    val finalInfluenceGraph = _influenceInTotal(influenceGraph)
    finalInfluenceGraph
    //finalInfluenceGraph size: vertices:93523 edges:1850050
  }

  /**
    * Author:weiwenda
    * Description: 收集14项融合因子
    * Date:9:59 2018/3/29
    */
  def collectNeighborInfo(fullTpin: Graph[InfluVertexAttr, Seq[Double]]) = {
    val sqlContext = session
    import sqlContext.implicits._
    val tpin = ConstructInfn._removeIsolate(fullTpin).
      joinVertices(_getScore()) {
        case (vid, attr, scoreAndwtbz) =>
          attr.xyfz = scoreAndwtbz._1.toInt
          attr.wtbz = scoreAndwtbz._2
          attr
      }.
      mapVertices((vid, vattr) => (vattr.xyfz, vattr.wtbz))
    val fzMessage = tpin.aggregateMessages[Seq[((Int, Boolean), Seq[Double])]](ctx =>
      if (ctx.srcAttr._1 >= 0 && ctx.dstAttr._1 >= 0) {
        ctx.sendToDst(Seq((ctx.srcAttr, ctx.attr)))
        ctx.sendToSrc(Seq((ctx.dstAttr, ctx.attr)))
      }, _ ++ _).cache()
    val nei_info = fzMessage.map { case (vid, list) =>
      val dfsl = list.filter(e => e._1._1 >= 0 && e._1._1 < 70).size
      val ycsl = list.filter(e => e._1._2).size
      val nei_num = list.size.toDouble
      val nei_mean = list.map(_._1._1).sum / nei_num
      val nei_wais = for {
        i <- Range(0, list(0)._2.size)
        totalWeight2 = list.map(_._2(i)).sum
        wais = list.map { case ((cur_fx, wtbz), weightList) => cur_fx * weightList(i) / totalWeight2 }.sum
      } yield wais
      //annotation of david:分别代表融合因子:低分企业数量,异常占比,,NAIS,NAWIS
      (vid, (dfsl, ycsl, nei_num, nei_mean, nei_wais))
    }
    val toReturn = tpin.
      vertices.
      join(nei_info).map { case (vid, ((xyfz, wtbz), (dfsl, ycsl, qysl, nais, nwais))) =>
      Correlation(vid, if (wtbz) 1 else 0, xyfz, nais, nwais(0), nwais(1), nwais(2), nwais(3), nwais(4), nwais(5), nwais(6), nwais(7), nwais(8), nwais(9), nwais(10), ycsl, dfsl, qysl.toInt)
    }.toDF
    //    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    //    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    toReturn
  }

  /**
    * Author:weiwenda
    * Description:根据皮尔逊相关系数选择特征
    * Date:15:27 2018/3/29
    */
  def featureSelect(init: DataFrame) = {
    val sqlContext = session
    import sqlContext.implicits._
    val wrong = init.filter($"wtbz" === 1)
    val good = init.filter($"wtbz" === 0)
    val goodSample = good.sample(true, wrong.count / good.count.toDouble)
    val balanced = wrong.union(goodSample)
    val assembler = new VectorAssembler()
      .setInputCols(Array("wtbz", "xyfz", "nais", "nwais0", "nwais1", "nwais2", "nwais3", "nwais4", "nwais5", "nwais6", "nwais7", "nwais8", "nwais9", "nwais10", "ycsl", "dfsl", "qysl"))
      .setOutputCol("features")
    val pr = assembler.transform(balanced)
    val Row(coeff1: Matrix) = CorrelationStat.corr(pr, "features").head
    coeff1
  }
  /**
    * Author:weiwenda
    * Description:收集13项融合因子
    * Date:17:07 2018/4/13
    */
  def computeVNFeature(influTpin: Graph[InfluVertexAttr, InfluEdgeAttr],
                       fullTpin: Graph[InfluVertexAttr, Seq[Double]],
                       direction: EdgeDirection = EdgeDirection.Out) = {
    val sqlContext = session
    import sqlContext.implicits._
    val tpin = ConstructInfn._removeIsolate(fullTpin).
      outerJoinVertices(_getScore()) {
        case (vid, attr, opt) =>
          if (!opt.isEmpty)
            (opt.get._1, opt.get._2)
          else
            (Double.NaN, false)
      }
    val nwapte = tpin.
      aggregateMessages[Seq[((Double, Boolean), Seq[Double])]](ctx =>
      direction match {
        case EdgeDirection.Both =>
          ctx.sendToDst(Seq((ctx.srcAttr, ctx.attr)))
          ctx.sendToSrc(Seq((ctx.dstAttr, ctx.attr)))
        case EdgeDirection.In =>
          ctx.sendToDst(Seq((ctx.srcAttr, ctx.attr)))
        case EdgeDirection.Out =>
          ctx.sendToSrc(Seq((ctx.dstAttr, ctx.attr)))
      }, _ ++ _).
      map { case (vid, list) =>
        val totalWeight2 = list.map(_._2(0)).sum
        val wapte = list.map { case ((cur_fx, wtbz), weightList) => cur_fx * weightList(0) / totalWeight2 }.sum
        //annotation of david:分别代表融合因子:低分企业数量,异常占比,,NAIS,NAWIS
        (vid, wapte)
      }
    val degreesRDD = tpin.degrees.cache()
    val triCountGraph = tpin.triangleCount()
    val maxTrisGraph = degreesRDD.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoef = triCountGraph.vertices.innerJoin(maxTrisGraph) {
      case (vertexId, triCount, maxTris) =>
        if (maxTris == 0) 0 else triCount / maxTris
    }
    val neighborCount = influTpin.
      outerJoinVertices(_getScore()) {
        case (vid, attr, opt) =>
          if (!opt.isEmpty)
            attr.wtbz = opt.get._2
          else
            attr.wtbz = false
          (attr, if (!opt.isEmpty) opt.get._1 else 0D)
      }.
      aggregateMessages[Seq[Int]](ctx => {
      ctx.sendToDst(Seq(
        if (ctx.attr.kg_bl > 0) 1 else 0,
        if (ctx.attr.tz_bl > 0) 1 else 0,
        if (ctx.attr.jy_bl > 0) 1 else 0, 0,
        if (ctx.attr.il_bl > 0) 1 else 0, 0, 1,
        if (ctx.srcAttr._1.wtbz) 1 else 0, if (ctx.srcAttr._1.wtbz) 0 else 1,
        if (ctx.srcAttr._2 > 0.5) 1 else 0, if (ctx.srcAttr._2 <= 0.5) 1 else 0))
      ctx.sendToSrc(Seq(
        0,
        0,
        0, if (ctx.attr.jy_bl > 0) 1 else 0,
        0, 1, 0,
        if (ctx.attr.il_bl > 0) 0 else if (ctx.dstAttr._1.wtbz) 1 else 0,
        if (ctx.attr.il_bl > 0) 0 else if (ctx.dstAttr._1.wtbz) 0 else 1,
        if (ctx.attr.il_bl > 0) 0 else if (ctx.dstAttr._2 > 0.5) 1 else 0,
        if (ctx.attr.il_bl > 0) 0 else if (ctx.dstAttr._2 <= 0.5) 1 else 0)
      )
    }, (a, b) => a.zip(b).map { case (c1, c2) => c1 + c2 })

    val toReturn = tpin.
      vertices.
      join(nwapte).join(neighborCount).join(clusterCoef).map { case (vid, ((((pte, wtbz), nwapte), narr), cluster)) =>
      VNFeature(vid, pte, if (wtbz) 1 else 0, nwapte, narr(0), narr(1), narr(2), narr(3), narr(4), narr(5), narr(6), narr(7), narr(8),
        narr(9), narr(10), cluster)
    }.toDF
    //    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    //    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    toReturn
  }

}
