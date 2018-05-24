import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.collection.Iterable
import scala.collection.mutable.HashSet
import java.nio.file.{Paths, Files}
import java.io.{File, FileOutputStream}
import java.io.PrintWriter
import scala.io.Source
import util.Try

object Util {

  // log the message, and print to log file if specified.
  def log(msg: String, logfile: String="") = {
    println(msg)
    if(logfile != "") {
      val pw = new PrintWriter(new FileOutputStream(new File(logfile),true))
      pw.write(msg + "\n")
      pw.close()
    }
  }

  def reformatRules(rules_with_stats_path: String,  // output rules with rule id and support / conf
                    rules_mapped_path:String,  // mapped rule ids + h + body1/body2 etc
                    predicateMapPath: String,  // mapped predicates to int
                    reformatRules_outpath: String //  output path of the reformated rules with string predicates and confidence scores
                    ) = {
    val predicateMap = Source.fromFile(predicateMapPath).getLines
                             .map(line => line.split(" "))
                             .map(arr => (arr(1).toInt -> arr(0)))
                             .toMap

    val rulesMap = Source.fromFile(rules_mapped_path).getLines
                         .map(line => line.split(" ").map(_.toInt))
                         .map(arr => (arr(0) -> arr.slice(1, arr.size)))
                         .toMap

    val rulesStats = Source.fromFile(rules_with_stats_path).getLines
                       .map(line => line.split(" ").map(_.toInt))
                       .map( arr => (rulesMap.getOrElse(arr(0), Array(-1, -1)), arr) ).toList
                       .map( { case (arr1, arr2) => arr1.map(pred => predicateMap.getOrElse(pred.toInt, "UnKnown Pred")).mkString(" ")  + " " + arr2.mkString(" ") } )

    val pw = new PrintWriter(new FileOutputStream(new File(reformatRules_outpath),true))
    rulesStats.foreach(l => pw.write(l + "\n"))
    
    pw.close()
  }

  def writeArrayToFile(arr: Array[String], outfile: String) {
    val pw = new PrintWriter(new File(outfile))
    for(a_rec <- arr) {
      pw.write(a_rec + "\n")
    }
    pw.close()
  }

  def mkdir(path_to_folder: String) = {
    val path = Paths.get(path_to_folder)
    Files.createDirectory(path)
  }
}