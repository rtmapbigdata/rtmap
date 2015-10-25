package com.rtmap.utils

/**
 * Created by skp on 2015/10/13.
 */
object ConfUtil {

/*wifiReport.conf*/
  //final val zkQuorum = "r1s4:2181,r1s5:2181,r2s5:2181"
  final val wifiReportZkQuorum = "r1s4:2181,r1s5:2181,r2s5:2181"
  final val wifiReportGroup = "1"
  final val wifiReportTopics = "test"
  final val wifiReportNumThreads = 1
  final val wifiReportTimes = 30

/*flightReport.conf*/
  final val flightReportTopics = "lkxxb_1,barcode_1,ajxxb_1"
  final val flightReportZkQuorum = "r1s4:2181,r1s5:2181,r2s5:2181"
  final val flightReportGroup = "1"
  final val flightReportNumThreads = 1
  final val flightReortTimes = 30

  final val poly = Array((317000,389000),(328000,389000),(317000,402000),(323000,408000),(323000,415000),(321000,416000),(321000,419000),(315900,419000),(348000,427000),(360000,428000),(360000,455000),(348000,455000),(328000,491000),(318000,492000),(318000,481000),(323000,475000),(323000,467000),(321000,467000),(320000,463000),(316000,463000))

}
