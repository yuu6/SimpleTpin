package lite.entity.impl

import lite.entity.VertexAttr
import org.apache.commons.lang3.StringUtils

/**
  * Created by weiwenda on 2017/3/15.
  */
class InfluVertexAttr(nsrdzdah: Long, name: String)
  extends BasicVertexAttr(nsrdzdah.toString, name, false) {
  var nsrdzah_lose:Boolean = false
}

object InfluVertexAttr {
  def apply(nsrdzdah: String, name: String): InfluVertexAttr = {
    if(StringUtils.isNumeric(nsrdzdah))
      new InfluVertexAttr(BigDecimal(nsrdzdah).longValue(), name)
    else{
      val attr = new InfluVertexAttr(Long.MaxValue, name)
      attr.nsrdzah_lose =true
      attr
    }
  }
}
