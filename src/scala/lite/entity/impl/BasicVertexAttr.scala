package lite.entity.impl

import lite.entity.VertexAttr

/**
  * Created by Administrator on 2016/4/27.
  */
class BasicVertexAttr(name: String,zjhm: String, var ishuman: Boolean)
  extends VertexAttr(name,zjhm) with Serializable {

  var xydj: String = ""
  var xyfz: Int = 0
  var wtbz: Boolean = false

}

object BasicVertexAttr {
  def apply(name: String, nsrsbh: String, ishuman: Boolean) = {
    new BasicVertexAttr(name, nsrsbh.replace(".0", ""), ishuman)
  }

  def combineNSRSBH(name1: String, name2: String): String = {
    var name = ""
    if (name1 != null) {
      // 拆分
      val name1s = name1.split(";")
      for (name1 <- name1s) {
        if (!name.contains(name1)) {
          if (name != "") {
            // 合并
            name = name + ";" + name1
          } else {
            name = name1
          }
        }
      }
    }
    if (name2 != null) {
      // 拆分
      val name2s = name2.split(";")
      for (name2 <- name2s) {
        if (!name.contains(name2)) {
          if (name != "") {
            // 合并
            name = name + ";" + name2
          } else {
            name = name2
          }
        }
      }
    }
    name
  }

  //annotation of david:尽可能判断为企业
  def combine(a: BasicVertexAttr, b: BasicVertexAttr) = {
    BasicVertexAttr(combineNSRSBH(a.name, b.name), combineNSRSBH(a.zjhm, b.zjhm), a.ishuman && b.ishuman)
  }
}

