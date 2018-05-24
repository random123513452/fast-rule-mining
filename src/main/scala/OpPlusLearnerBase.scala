import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import org.apache.spark.SparkContext
import java.nio.file.{Paths, Files}
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import util.Random

/*
Base class for OpPlus batch rule mining
*/

@SerialVersionUID(171L)
abstract class OpPlusLearnerBase extends Serializable {


  // given facts in RDD, and path to rules (not pruned)
  // get the rule metrics (supp and # total predictions) for rules after pruning
  def mine(sc:SparkContext, facts: RDD[Array[Int]], cand_rules_path:String, output_root:String, num_parts: Int=1): RDD[Array[Int]] = {
    val rules = Source.fromFile(cand_rules_path).getLines()
                  .map(rule => rule.split(" ").map(_.toInt))
                  .toArray

    // use the preds to filter out the rules that contains body pred not in facts to avoid unnecessary inference
    val preds = sc.textFile(output_root + "/preds")
                  .map({ case l => l.toInt })
                  .collect().toSet
    val rules_filtered = rules//.filter({ case rule => filterRules(rule, preds) })

    Util.log("Preds size: %d, rules size: %d, rules_filtered size: %d".format(preds.size, rules.size, rules_filtered.size))

    val h1 = sc.textFile(output_root + "/h1", NUM_COUNT_TASKS)
                         .map(line => line.split(" ").map(_.toInt))

    val h2 = sc.textFile(output_root + "/h2", NUM_COUNT_TASKS)
                         .map(line => line.split(" ").map(_.toInt))

    val pruned_rules = prune(sc.broadcast(rules_filtered), h1, h2)

    Util.log(" filtered_rules size: " + rules_filtered.size + "  pruned_rules size: " + pruned_rules.size)

    val cand_rules_rdd = sc.textFile(cand_rules_path, NUM_INPUT_TASKS)
                           .map{ line => (line.split(" ")(0).toInt, line) }

    // randomly parition the pruned rules to parts to reduce mem spill if there is any.
    val num_per_part = math.ceil(pruned_rules.size / num_parts + 1).toInt
    val partitioned_rules = Random.shuffle(pruned_rules.toList).toArray.grouped(num_per_part).toArray

    Util.log("Partitioning prune rules randomly into " + num_parts + " part(s), each with max # of rules: " + num_per_part)

    var res = ArrayBuffer[Array[Int]]()
    var curr_part_index: Int = 1
    
    for (part_rules <- partitioned_rules) {
      Util.log("processing part: " + curr_part_index)
      curr_part_index += 1
      res = res ++ applyRules(sc, facts, sc.broadcast(part_rules))
                    .distinct() // comment this line to get the new metric conf
                    .leftOuterJoin( facts.map({ case Array(h, x, y) => ((h, x, y), BASE_FACT) }),  NUM_JOIN_TASKS_LARGE * 2)
                    .map{ case (_, (id, base_exist)) => (id, (if(base_exist == None) 0 else 1, 1)) }
                    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2), NUM_COUNT_TASKS)
                    .map{ case (id, (num, cnt)) => (id, (num, cnt)) }
                    .join(cand_rules_rdd, NUM_INPUT_TASKS)
                    .map{ case (id, ((num, cnt), line)) => line.split(" ").map(_.toInt) ++ Array(num, cnt) }
                    .collect
    }
    sc.parallelize(res)
  }

  // Given (high-confidence) rules, and facts, apply the rules to given facts to get inferred facts (predictions).
  // Inferred facts will have confidence same as the inferring rule, 
  // if there are multiple rules inferring the same fact, highest confidence.

  def infer(sc:SparkContext, facts: RDD[Array[Int]], rules_with_stats: Array[Array[Int]]) : RDD[(Int, Int, Int, Float, Int)] = {
    
    val rules_rdd = sc.parallelize(rules_with_stats, NUM_INPUT_TASKS)
                      .map( arr => (arr(0), arr(arr.size - 2).toFloat / arr(arr.size-1).toFloat) )

    // remove supp / conf to run with legacy code
    val rules = rules_with_stats.map( arr => arr.slice(0, arr.size - 2))

    applyRules(sc, facts, sc.broadcast(rules))
      .map{ case ((h, x, y), rid) => (rid, (h, x, y)) }
      .join(rules_rdd, NUM_INPUT_TASKS)
      .map{ case (rid, ((h, x, y), conf)) => ((h, x, y), (conf, rid))  }
      .reduceByKey( (t1, t2) => if (t1._1 > t2._1) t1 else t2 ) // remove duplicate predictions by different rules.
      .map{ case ((h, x, y), (conf, rid)) => (h, x, y, conf, rid)}
  }

  // filtering a rule, the preds of the facts must contain all the body preds, otherwise there would be instantiations.
  private def filterRules(rule: Array[Int], preds: Set[Int]) = {
    // length 2 rules
    if (rule.size == 3) {
      preds.contains(rule(2))
    } else {
      preds.contains(rule(2)) && preds.contains(rule(3))
    }
  }

  // given h1 h2 that exceesds the contraint, prune rules and return the rules after pruning to use.
  protected def prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]): Array[Array[Int]]

  // given rules and facts, apply the inference rules to facts to get rdd of ((h, x, y), rid)
  protected def applyRules(sc:SparkContext, facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]]): RDD[((Int, Int, Int), Int)] = {

    val max_group_size = 30 * 1000

    val (facts1, facts2) = getMappedFacts(sc, facts, rules.value)


    val facts1_grouped = facts1.aggregateByKey(Array[(Int, Int)](), NUM_CHECK_TASKS)(_ :+ _, _ ++ _)
          .flatMap({ case (z, arr) =>
              ( for ( arr_small <- arr.grouped(max_group_size)) yield(z, arr_small)  )
           })
    
    val facts2_grouped = facts2.aggregateByKey(Array[(Int, Int)](), NUM_CHECK_TASKS)(_ :+ _, _ ++ _)
          .flatMap({ case (z, arr) =>
              ( for ( arr_small <- arr.grouped(max_group_size)) yield(z, arr_small)  )
           })

    val rulesTable_q_r = rules.value.groupBy({case Array(id, h, q, r) => (q, r)})

    facts1_grouped.join(facts2_grouped, NUM_JOIN_TASKS_LARGE * 4)
          .repartition(NUM_JOIN_TASKS_LARGE * 8) // use very very large # to avoid OOM
          .flatMap({ case (z, (arr1, arr2)) => 
              check_join_adaptive(arr1, arr2, rules.value, rulesTable_q_r)
          })
  }

  protected def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]): (RDD[(Int, (Int, Int))], RDD[(Int, (Int, Int))])

  // adaptive check for 
  protected def check_join_adaptive(facts1: Array[(Int, Int)], facts2: Array[(Int, Int)], rules: Array[Array[Int]], rules_q_r: Map[(Int, Int),Array[Array[Int]]]) = {
    if(facts1.size * facts2.size < 10 * rules.size)
      check_join_facts(facts1, facts2, rules_q_r)
    else
      check_join_rules(facts1, facts2, rules)
  }

  private def check_join_facts(facts1: Array[(Int, Int)], facts2: Array[(Int, Int)], rules_q_r: Map[(Int, Int),Array[Array[Int]]]) = {
    facts1.toIterator.flatMap({
     case (q, x) =>
        facts2.toIterator.flatMap({
          case (r, y) =>
            rules_q_r.getOrElse((q, r), Array.empty[Array[Int]]).toIterator.map({
              case Array(rid, h, q, r) => ((h, x, y), rid)  }) }) })
  }

  private def check_join_rules(facts1: Iterable[(Int, Int)], facts2: Iterable[(Int, Int)], rules: Array[Array[Int]]) = {
    val predMap1 = facts1.groupBy(_._1)
    val predMap2 = facts2.groupBy(_._1)

    rules.toIterator.flatMap({
       case Array(id, h, q, r) =>
          predMap1.getOrElse(q, Nil).flatMap({
             case (q, x) =>
                predMap2.getOrElse(r, Nil).map({
                   case (r, y) => ((h, x, y), id)
                }) })})
  }


   // Special ID used to indicate a fact coming from the original knowledge base.
  // This must not be a possible rule ID.
  protected val BASE_FACT = 0

  // on yago2s , we need much smaller parallelism to avoid overhead
  // on freebase, please change scale_factor to 4
  protected val scale_factor = 1

    // Spark configurations.
  protected val NUM_INPUT_TASKS = 64 * scale_factor
  protected val NUM_JOIN_TASKS = 128 * scale_factor
  protected val NUM_CHECK_TASKS = 64 * scale_factor
  protected val NUM_COUNT_TASKS = 64 * scale_factor

  protected val NUM_JOIN_TASKS_LARGE = 256 * scale_factor
}
