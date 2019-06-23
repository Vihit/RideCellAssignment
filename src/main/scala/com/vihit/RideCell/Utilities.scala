package com.vihit.RideCell

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Utilities {
  
  def splitColumns(numCols:Int, column:String, separator:String, splitCols:Array[String]):Array[Column] = {
    
    val splitColumns = for(i <- 0 to numCols) yield split(col(column),separator)(i)
    splitColumns.zip(splitCols).map(f=>f._1.as(f._2)).toArray
  }
  
  def concatColumns(separator:String, columns:Array[String], outputColName:String):String = {
    print("concat("+columns.mkString(separator)+") as "+outputColName)
    "concat("+columns.mkString(""",""""+separator+"""",""")+") as "+outputColName
  }
  
}