package com.czl.submitter.spark.entity

import java.io.File
import java.net.{URL => NetURL}
import java.util.regex.{Matcher, Pattern}

/**
 * Author: CHEN ZHI LING
 * Date: 2022/6/6
 * Description:
 */
class SparkInfo(val sparkHome: String) extends Serializable {


  private[this] lazy val SPARK_SCALA_VERSION_PATTERN: Pattern = Pattern.compile("^spark-core_(.*)-(.*).jar$")


  lazy val scalaVersion: String = {
    val matcher: Matcher = SPARK_SCALA_VERSION_PATTERN.matcher(sparCoreJar.getName)
    matcher.matches()
    matcher.group(1)
  }


  lazy val sparkVersion: String = {
    val matcher: Matcher = SPARK_SCALA_VERSION_PATTERN.matcher(sparCoreJar.getName)
    matcher.matches()
    matcher.group(2)
  }


  lazy val sparkLib: File = {
    require(sparkHome != null, "sparkHome can not be null")
    require(new File(sparkHome).exists(), "sparkHome must be existed")
    val lib = new File(s"$sparkHome/jars")
    require(lib.exists() && lib.isDirectory, s"$sparkHome/jars must be directory")
    lib
  }


  lazy val sparkLibs: List[NetURL] = sparkLib.listFiles().map((_: File).toURI.toURL).toList



  lazy val sparCoreJar: File = {
    val distJar: Array[File] = sparkLib.listFiles().filter((_: File).getName.matches("spark-core_.*\\.jar"))
    distJar match {
      case x if x.isEmpty =>
        throw new IllegalArgumentException(s"$sparkLib dose note have spark-core.jar")
      case x if x.length > 1 =>
        throw new IllegalArgumentException(s"$sparkLib has more than one spark-core.jar")
      case _ =>
    }
    distJar.head
  }
}
