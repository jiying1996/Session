package spark

import java.net.URL
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.qf.sessionanalyze.util.DateUtils

object Test {
  def main(args: Array[String]): Unit = {
//    val url = "https://cloud.tencent.com/developer/ask/41069"
//    val t = new URL(url)
//    println(t.getFile.split("/")(3))
//    println(t.getHost)
    val timeStamp = "1545033177995"
    val fm = DateUtils.formatDate(new Date(timeStamp.toLong))
    
    println(fm)
  }
}
