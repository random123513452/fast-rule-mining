import java.io.File
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashSet
import scala.io.Source
import java.nio.file.{Paths, Files}



object Main {

  private val conf = new SparkConf().setAppName("Fast Rule Mining")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  private val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  private val logger = Logger.getLogger(getClass.getName)

  private val Leaners = Array(
    OpPlusLearnerType1,
    OpPlusLearnerType1, OpPlusLearnerType2, OpPlusLearnerType3, OpPlusLearnerType4, OpPlusLearnerType5, OpPlusLearnerType6
  )

  val constraint = 100
  val rule_types = Array(1, 2, 3, 4, 5, 6)

  def main(args: Array[String]): Unit = {

    Util.log("\n\n\n\nArgs: " + args.mkString(" "))


      val facts_path = args(0)
      val schema_path = args(1)
      val cand_rules_path_base =  args(2)
      val output_root= args(3)  
      val logfile = args(4) 

      val facts_str = sc.textFile(facts_path, NUM_INPUT_TASKS)
                    .distinct().map(line => line.split("\\s+"))
      
      Util.log("Propressing intput .... ")
      preprocess(sc, facts_str, cand_rules_path_base, schema_path, output_root)


      val facts_int_path = output_root + "/facts_int/*"
      val facts = sc.textFile(facts_int_path, NUM_INPUT_TASKS)
                    .distinct().map(line => line.split("\\s+").map(_.toInt))

      val cand_rules_int_path = output_root + "/cand_rules_int-%d"
      mineRules(sc, facts, cand_rules_int_path, output_root, logfile)

      Util.log("Postprocessing output rules ...")
      postProcess(output_root)
      Util.log("All processing Done, rules outputed in: " + output_root + "/output_rules_str-%d")
  }

  // Given candidate rules of type in rule_types, and facts, get rule stats for each rule and output to designated paths.
  def mineRules(sc: SparkContext, facts: RDD[Array[Int]], cand_rules_path: String, output_root:String, logfile: String) = {
      val rules_path_base =  cand_rules_path //"/opt/workspace/ontological-pathfinding/data/YAGOData/YAGORules.csv-%d.map"

      val Msg = "Mining rules' stats from cand_rules_path " + cand_rules_path + ", outputting resulting rules to: " + output_root
      Util.log("\n\n\n" + Msg, logfile)

      val t0 = System.currentTimeMillis()
      populateHistogram(sc, facts, output_root)
      val t1 = System.currentTimeMillis()
      Util.log("populateHistogram Done, time spent: " + (t1 - t0)/1e3d + "\n", logfile)

      for(rule_type <- rule_types) {
        val start = System.currentTimeMillis()

        val rules_path=rules_path_base.format(rule_type)
        Util.log("Processing rules: " + rules_path)

        val rules_stats_array = Leaners(rule_type).mine(sc, facts, rules_path, output_root)
                                            .map{ case arr => arr.mkString(" ") }
                                            .collect()
        Util.writeArrayToFile(rules_stats_array, output_root + "/out_rules_int-" + rule_type)

        val end = System.currentTimeMillis()
        Util.log("processing for rule type " + rule_type + " done, time spent:" + (end - start)/1e3d + "\n", logfile)
      }

      val t2 = System.currentTimeMillis()
      Util.log("All mining done, time spent:" + (t2 - t0)/1e3d, logfile)
  }
  
  // Given input facts, get the h1, h2 that exceeds functional contrant to 
  // output_root/h1, output/h2 respectively.
  private def populateHistogram(sc:SparkContext, facts: RDD[Array[Int]], output_root:String) = {

    facts.cache()

    facts.map({ case Array(h, x, y) => ((h, x), 1) })
         .reduceByKey(_ + _)
         .filter{ case (_, count) => count > constraint }
         .map{ case ((h, x), count) => Array(h, x).mkString(" ") }
         .saveAsTextFile(output_root + "/h1")

    facts.map({ case Array(h, x, y) => ((h, y), 1) })
         .reduceByKey(_ + _)
         .filter{ case (_, count) => count > constraint }
         .map{ case ((h, y), count) => Array(h, y).mkString(" ") }
         .saveAsTextFile(output_root + "/h2")
 
    // distinct preds
    facts.map{ case Array(h, x, y) => h }
         .distinct()
         .coalesce(16)
         .map({  case x => x.toString })
         .saveAsTextFile(output_root + "/preds")
  }

  // Convert all the facts and rules to integer representation, write to file, as well as those mappings
  // Output will be written to disk:
  // output_root/ 
  //           facts_int -- folder, facts in integers
  //           cand_rules_int-%d  -- files, candidate rules in integer representation
  //           predMap  -- file, mapping predicates from string to integer
  //           entMap -- folder, mappting entities from string to integer
  private def preprocess(sc:SparkContext, facts_str: RDD[Array[String]], cand_rules_path:String, schema_path: String, output_root: String) = {
    // mapping each predicate to an integer
    val predMap = Source.fromFile(schema_path).getLines().map(l => l.split("\\s+")(0)).toArray.distinct.zipWithIndex.map(t => (t._1, t._2 + 1)).toMap
    
    // mapping each entity to integer representation
    val entMap = facts_str.flatMap{ case Array(pred, sub, obj) => Array(sub, obj) }
                  .distinct.zipWithIndex.map{ case (ent, id) => (ent, id + 1) }

    entMap.saveAsTextFile(output_root + "/entMap")

    facts_str.map{ case Array(pred, sub, obj) => (predMap.getOrElse(pred, -1), sub, obj) }
         .map{case (pred, sub, obj) => (sub, (pred, obj))}
         .join(entMap)
         .map{ case (sub, ((pred, obj), sub_id)) => (obj, (pred, sub_id)) }
         .join(entMap)
         .map{ case (obj, ((pred, sub_id), obj_id)) => Array(pred, sub_id, obj_id).mkString(" ") }
         .saveAsTextFile(output_root + "/facts_int")

    // write predMap to disk
    Util.writeArrayToFile(predMap.toArray.map(t => t._1 + " " + t._2), output_root + "/predMap")

    // mapping each rule in string to integer, and assign rule id to those rules. write those rules to 
    for(rule_type <- rule_types) {
      val rules_path = cand_rules_path.format(rule_type)
      val rules_int = Source.fromFile(rules_path)
                   .getLines().map(l => l.split("\\s+")).toArray
                  .zipWithIndex
                  .map(t => (t._2 + rule_type * 100000000 + 1) + " " +  t._1.map(pred => predMap.getOrElse(pred, "UNDEFINED_PREDICATE")).mkString(" ") )
      val cand_rules_int_path = output_root + "/cand_rules_int-" + rule_type
      Util.writeArrayToFile(rules_int, cand_rules_int_path)
    }
  }

 // convert outputed rules in integer to string, and get the confidence socre. outputed rules will be written in "output_rules_str-%id"
  private def postProcess(output_root: String) = {
    val predMap = Source.fromFile(output_root + "/predMap").getLines().map(line => line.split("\\s+"))
                    .map(arr => (arr(1), arr(0))).toMap

    for(rule_type <- rule_types) {
      val rules_path = (output_root + "/out_rules_int-%d").format(rule_type)
      val rules_str = Source.fromFile(rules_path).getLines
                        .map(line => line.split("\\s+")).toArray
                        .map(arr => 
                          (arr.slice(1, arr.size - 2).map(id => predMap.getOrElse(id, "Undefined_predicate")), arr(arr.size - 2), arr(arr.size - 2).toFloat/arr(arr.size - 1).toFloat ))
                        .map(t => t._1.mkString(" ") + " " + t._2 + " " + t._3 )
      val out_rules_str_path = output_root + "/output_rules_str-" + rule_type

      Util.writeArrayToFile(rules_str, out_rules_str_path)
    }
  }


  protected val NUM_INPUT_TASKS = 128
}