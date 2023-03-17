package com.czl.submitter.flink.entity

import java.io.File
import java.net.{URL => NetURL}
import java.util.regex.{Matcher, Pattern}

/**
 * Author: CHEN ZHI LING
 * <br>
 * Date: 2022/5/12 16:10
 * <br>
 * Description: according flinkHome to get flink cluster version
 */
class FlinkInfo(val flinkHome: String) extends Serializable {



  private[this] lazy val FLINK_SCALA_VERSION_PATTERN: Pattern = Pattern.compile("^flink-dist_(.*)-(.*).jar$")


  lazy val fullVersion: String = s"${version}_$scalaVersion"


  lazy val scalaVersion: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(1)
  }


  lazy val version: String = {
    val matcher: Matcher = FLINK_SCALA_VERSION_PATTERN.matcher(flinkDistJar.getName)
    matcher.matches()
    matcher.group(2)
  }


  lazy val flinkLib: File = {
    require(flinkHome != null, "flinkHome can not be null")
    require(new File(flinkHome).exists(), "flinkHome must be existed")
    val lib = new File(s"$flinkHome/lib")
    require(lib.exists() && lib.isDirectory, s"$flinkHome/lib must be directory")
    lib
  }


  lazy val flinkLibs: List[NetURL] = flinkLib.listFiles().map((_: File).toURI.toURL).toList



  lazy val flinkYarnShipFiles: List[String] = {
    List(flinkLib.toString,s"$flinkHome/plugins",s"$flinkHome/conf/log4j.properties")
  }



  lazy val flinkDistJar: File = {
    val distJar: Array[File] = flinkLib.listFiles().filter((_: File).getName.matches("flink-dist_.*\\.jar"))
    distJar match {
      case x if x.isEmpty =>
        throw new IllegalArgumentException(s"$flinkLib does not have flink-dist.jar")
      case x if x.length > 1 =>
        throw new IllegalArgumentException(s"$flinkLib has more than one flink-dist.jar")
      case _ =>
    }
    distJar.head
  }
}
