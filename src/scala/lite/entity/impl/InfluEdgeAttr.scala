package lite.entity.impl

import java.text.DecimalFormat

import lite.entity.EdgeAttr

/**
  * Created by weiwenda on 2017/3/15.
  */
class InfluEdgeAttr extends EdgeAttr with Serializable {
    var tz_bl: Double = 0.0
    var jy_bl: Double = 0.0
    var kg_bl: Double = 0.0
    var il_bl: Double = 0.0
}

object InfluEdgeAttr{
    def apply() = {
        new InfluEdgeAttr()
    }

    def combine(a: InfluEdgeAttr, b: InfluEdgeAttr): InfluEdgeAttr = {
        a.kg_bl += b.kg_bl
        a.jy_bl += b.jy_bl
        a.tz_bl += b.tz_bl
        a.il_bl += b.il_bl
        a
    }
}
