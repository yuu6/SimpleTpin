package lite.entity.impl

import lite.entity.EdgeAttr

/**
  * Created by Administrator on 2016/4/27.
  */
class BasicEdgeAttr(var w_control: Double,
                    var w_tz: Double,
                    var w_gd: Double,
                    var w_trade: Double
                   ) extends EdgeAttr{
  //annotation of david:交易金额、投资金额、税率、税额
  var trade_je: Double = 0.0
  var tz_je: Double = 0.0
  var taxrate: Double = 0.0
  var se: Double = 0.0
  //annotation of david:互锁边标识位
  var is_IL: Boolean = false
  var w_IL: Double = 0D

  //annotation of david:前件路径
  def isAntecedent(weight: Double = 0.0): Boolean = {
    if (this.is_IL) return false
    //annotation of david:严格的，当weight取值为0.0时，允许等号将导致交易边被 count in
    (this.w_control > weight || this.w_tz > weight || this.w_gd > weight)
  }

  def isTrade(): Boolean = {
    this.w_trade != 0.0
  }
}

object BasicEdgeAttr {
  def combine(a: BasicEdgeAttr, b: BasicEdgeAttr) = {
    val toReturn = new BasicEdgeAttr(a.w_control + b.w_control, a.w_tz + b.w_tz, a.w_gd + b.w_gd, a.w_trade + b.w_trade)
    toReturn.trade_je = a.trade_je + b.trade_je
    toReturn.tz_je = a.tz_je + b.tz_je
    toReturn.se = a.se + b.se
    toReturn.w_IL = a.w_IL + b.w_IL;
    toReturn.is_IL = a.is_IL || b.is_IL;
    toReturn
  }

  def apply(w_control: Double = 0.0, w_tz: Double = 0.0, w_gd: Double = 0.0, w_trade: Double = 0.0) = {
    val lw_control = if (w_control > 1.0) 1.0 else w_control
    val lw_tz = if (w_tz > 1.0) 1.0 else w_tz
    val lw_trade = if (w_trade > 1.0) 1.0 else w_trade
    val lw_gd = if (w_gd > 1.0) 1.0 else w_gd
    new BasicEdgeAttr(lw_control, lw_tz, lw_gd, lw_trade)
  }
}