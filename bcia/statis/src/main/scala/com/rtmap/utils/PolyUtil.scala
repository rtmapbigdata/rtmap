package com.rtmap.utils


/**
 * Created by skp on 2015/10/12.
 * pt:(x,y),poly:((x,y),(xy),(......))
 */
object PolyUtil {
  def isInsidePolygon(pt: Tuple2[Int,Int],poly: Array[Tuple2[Int,Int]]) = {
    var c = -1
    var i = -1
    val l = poly.length
    var j = l - 1
    while (i < l-1) {
      i += 1
      if ((poly(i)._1 <= pt._1 && pt._1 < poly(j)._1) || (poly(j)._1 <= pt._1 && pt._1 < poly(i)._1))
      {
        if (pt._2 <(poly(j)._2 -poly(i)._2 ) *(pt._1 -poly(i)._1 ) /(poly(j)._1 -poly(i)._1 ) +poly(i)._2 )
        {c = -c}
      }
      j = i
    }
    c
  }

}
