package cn.ac.iie.iaa

import java.util.regex.Matcher
import java.util.regex.Pattern

import scala.util.Try

object InternalProcess extends Serializable {
  def  searchBinary(arr: Array[(Long, Long, String)], low: Int, high: Int, ipLong: Long): Int = {
    if (low > high) {
      return -1;
    } else {
      var mid = (low + high) / 2;
      if (arr(mid)._1 <= ipLong && ipLong <= arr(mid)._2) {
        return mid;
      } else if (ipLong < arr(mid)._1) {
        return searchBinary(arr, low, mid - 1, ipLong);
      } else {
        return searchBinary(arr, mid + 1, high, ipLong);
      }
    }

  }

    def getUserType(str : String, broadCast : Array[(Long, Long, String)]) : String = {
      val ipLong = IPv4ToLong(str)
      var index : Int = -1
      var userType : String = "-1"
      index = searchBinary(broadCast, 0, broadCast.length - 1, ipLong)
      if(index != -1){
        broadCast(index)._3
      }
      else
        {"-1"}
  }



  def IPv4ToLong(dottedIP: String): Long = {
    var num: Long = 0
    var i: Int = 0
    if (dottedIP == "") num = -1 //源ip为空判断
    else {
      val addrArray: Array[String] = dottedIP.split("\\.")
      while (i < addrArray.length) {
        val power: Int = 3 - i
        Try(num = num + ((addrArray(i).toInt % 256) * Math.pow(256, power)).toLong).getOrElse()
        i += 1
      }
    }
    num
  }

  //IP处理
  def GetFirstIP(str: String): String = {
    if (!("".equals(str))) {
      if (!(str.contains(";")))
        str;
      else if ((str.trim().split(";") != null) && (str.trim().split(";").length > 0)){
        str.trim().split(";")(0)
      }
      else "-1"
    } else "-1"
  }

  //域名有效性判断
  def ValidDomian(str: String): String = {
    var flag = false
    var domain = ""
    val result = "0"
    if (str != "") {
      domain = str.trim().toLowerCase()
      if ((domain.endsWith(".")) || (domain.endsWith("?")))
        domain = domain.substring(0, domain.length() - 1)
      if (domain.length() <= 256) {
        val pattern: Pattern = Pattern.compile("^[0-9a-zA-Z]+[0-9a-zA-Z\\.-]*\\.[a-zA-Z]{2,4}$")
        val matcher: Matcher = pattern.matcher(domain)
        flag = matcher.matches()
        if (flag)
          domain
        else
          result
      } else result
    } else result
  }

  //获取泛域名以及域名级别
  private def DomainAndlevels(url: String): Array[String] = {
    var results = new Array[String](3)
    try {
      val p: Pattern = Pattern.compile("[^.]*\\.(com|cn|edu|net|org|biz|info|gov|pro|name|museum|coop|aero|idv|tv|co|tm|lu|me|cc|mobi|fm|sh|la|dj|hu|so|us|hk|to|tw|ly|tl|in|es|jp|mx|vc|io|am|sc|cm|pw|de|sg|cd|fr|arpa|asia|im|tel|mil|jobs|travel)(\\..*)?$", 2);
      val m: Matcher = p.matcher(url)
      if (m.find()) {
        results(0) = m.group().toString()
        results(2) = "0"
      }
      if (results(0) == null)
        results(0) = ""

      val newstr = results(0).split("\\.")
      var num = 0
      var a = 0
      var flag = false
      var flag1 = false
      var flag2 = false
      var first = ""
      var last = ""

      //取顶级域名->一级域名->二级域名...
      for (i <- 0 to (newstr.length - 1)) {
        if ((newstr(i).equals("com")) || (newstr(i).equals("cn")) ||
          (newstr(i).equals("edu")) || (newstr(i).equals("net")) ||
          (newstr(i).equals("org")) || (newstr(i).equals("biz")) ||
          (newstr(i).equals("info")) || (newstr(i).equals("gov")) ||
          (newstr(i).equals("pro")) || (newstr(i).equals("name")) ||
          (newstr(i).equals("museum")) || (newstr(i).equals("coop")) ||
          (newstr(i).equals("aero")) || (newstr(i).equals("idv")) ||
          (newstr(i).equals("tv")) || (newstr(i).equals("co")) ||
          (newstr(i).equals("tm")) ||
          (newstr(i).equals("lu")) || (newstr(i).equals("me")) ||
          (newstr(i).equals("cc")) || (newstr(i).equals("mobi")) ||
          (newstr(i).equals("fm")) || (newstr(i).equals("sh")) ||
          (newstr(i).equals("la")) || (newstr(i).equals("dj")) ||
          (newstr(i).equals("hu")) || (newstr(i).equals("so")) ||
          (newstr(i).equals("im")) || (newstr(i).equals("us")) ||
          (newstr(i).equals("hk")) || (newstr(i).equals("to")) ||
          (newstr(i).equals("tw")) || (newstr(i).equals("ly")) ||
          (newstr(i).equals("tl")) || (newstr(i).equals("in")) ||
          (newstr(i).equals("es")) || (newstr(i).equals("jp")) ||
          (newstr(i).equals("mx")) || (newstr(i).equals("tel")) ||
          (newstr(i).equals("vc")) || (newstr(i).equals("io")) ||
          (newstr(i).equals("am")) || (newstr(i).equals("sc")) ||
          (newstr(i).equals("cm")) || (newstr(i).equals("pw")) ||
          (newstr(i).equals("de")) || (newstr(i).equals("sg")) ||
          (newstr(i).equals("cd")) || (newstr(i).equals("fr")) ||
          (newstr(i).equals("arpa")) || (newstr(i).equals("asia")) ||
          (newstr(i).equals("im")) || (newstr(i).equals("tel")) ||
          (newstr(i).equals("mil")) || (newstr(i).equals("jobs")) ||
          (newstr(i).equals("travel"))) {

          num += 1
          flag1 = true
          last = newstr(i)
          if (i == newstr.length - 1)
            flag2 = true
        } else {
          flag1 = false
        }

        if ((num == 1) && (flag1)) {
          a = i;
          first = newstr(i)
        }
        if ((num == 2) && (i - a == 1)) {
          flag = true;
          first = first + "." + newstr(i)
        }
      }

      if ((results(0).contains("akadns.net")) ||
        (results(0).contains("edgekey.net")) ||
        (results(0).contains("akamaiedge.net")) ||
        (results(0).contains("hubspot.net")) ||
        (results(0).contains("cloudfront.net")) ||
        (results(0).contains("cdngs.net")) ||
        (results(0).contains("cdngc.net")) ||
        (results(0).contains("lxdns.com")) ||
        (results(0).contains("wscdns.com")) ||
        (results(0).contains("cdn20.com")) ||
        (results(0).contains("fastcdn.com")) ||
        (results(0).contains("fwcdn.net")) ||
        (results(0).contains("fwdns.net")) ||
        (results(0).contains("cloudcdn.net")) ||
        (results(0).contains("ccgslb.net")) ||
        (results(0).contains("chinacache.net")) ||
        (results(0).contains("llnwd.net")) ||
        (results(0).contains("edgecastcdn.net"))) {
        results(2) = "cname"
        results(0) = results(0).split(first)(0) + first;
      } else {
        if ((num > 2) || ((num == 2) && (!(flag)))) {
          var newurl: String = ""

          if (results(0).split(first).length > 2) {
            newurl = results(0).split(first)(1) + first +
              results(0).split(first)(2)

            results = DomainAndlevels(newurl)
          } else if ((flag2) && (first.equals(last))) {
            newurl = results(0).split(first)(1) + first;

            results = DomainAndlevels(newurl)
          } else {
            results = DomainAndlevels(results(0).split(first)(1))
          }

        }
      }
      val temp = url.split(results(0))
      if (temp.length != 0)
        results(1) = Integer.toString(temp(0).split("\\.").length + 1)
      else
        results(1) = "1"

      if (results(0).equals("")) {
        results(0) = "-1"
        results(1) = "-1"
        results(2) = "-1"
      }
      return results
    } catch {
      case e: Exception =>
        println("泛域名与原域名相同")
        results(0) = "-1"
        results(1) = "-1"
        results(2) = "-1"
    }
    return results
  }

  //域名信息获取
  def getDomainInfo(domain: String, str: String): String = {
    var res = new Array[String](3)
    var result = ""

    res = DomainAndlevels(domain);
    if ("domain".equals(str))
      result = res(0)
    else if ("level".equals(str))
      result = res(1)
    //else if ("cname".equals(str))
    // result = res(2)
    else
      result = res(0)
    return result
  }

  //五分钟统计
  def ComputeMin(string: String): String = {
    val minute = string.toInt
    minute match {
      case 0 | 1 | 2 | 3 | 4 => "01"
      case 5 | 6 | 7 | 8 | 9 => "02"
      case 10 | 11 | 12 | 13 | 14 => "03"
      case 15 | 16 | 17 | 18 | 19 => "04"
      case 20 | 21 | 22 | 23 | 24 => "05"
      case 25 | 26 | 27 | 28 | 29 => "06"
      case 30 | 31 | 32 | 33 | 34 => "07"
      case 35 | 36 | 37 | 38 | 39 => "08"
      case 40 | 41 | 42 | 43 | 44 => "09"
      case 45 | 46 | 47 | 48 | 49 => "10"
      case 50 | 51 | 52 | 53 | 54 => "11"
      case 55 | 56 | 57 | 58 | 59 => "12"
    }
  }
  
  
  
}