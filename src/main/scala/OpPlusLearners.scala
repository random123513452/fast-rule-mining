import org.apache.spark.rdd.RDD;
import org.apache.spark.broadcast.Broadcast
import scala.collection.Iterable;
import scala.collection.mutable.HashSet;
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap


/*Learner for type 1 rules: h(x, y) <- q(x, y) */
object OpPlusLearnerType1 extends OpPlusLearnerBase {
  protected override def  prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = rules.value

  protected override def applyRules(sc:SparkContext, facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]]) = {
    val rulesTable = rules.value.groupBy({case Array(id, head, body) => body})

    facts.flatMap({ case Array(pred, x, y) => 
      for( Array(id, h, q) <- rulesTable.getOrElse(pred, Array.empty[Array[Int]]) )  yield ((h, x, y), id)
     })
  }

  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = (null, null)
}

/*Learner for type 2 rules: h(x, y) <- q(y, x) */
object OpPlusLearnerType2 extends OpPlusLearnerBase {
  protected override def  prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = rules.value

  protected override def applyRules(sc:SparkContext, facts: RDD[Array[Int]], rules: Broadcast[Array[Array[Int]]]) = {
    val rulesTable = rules.value.groupBy({case Array(id, head, body) => body})

    facts.flatMap({ case Array(pred, y, x) => 
      for( Array(id, h, q) <- rulesTable.getOrElse(pred, Array.empty[Array[Int]]) )  yield ((h, x, y), id)
     })
  }

  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = (null, null)
}


/* Learner for type3 rule: h(x, y) <- q(z, x), r(z, y) */
object OpPlusLearnerType3 extends OpPlusLearnerBase {

  protected override def prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = {
        val filtered_rules = h1.map({case Array(pred, sub) => (sub, pred)})
                               .groupByKey(NUM_CHECK_TASKS)
                               .flatMap({case (sub, preds) => prune_join(preds, rules.value)})
                               .map({case Array(rid, h, b1, b2) => (rid, h, b1, b2)})
                               .distinct()
        val filtered_rules_set = filtered_rules.collect().toSet
        // debug 
        // filtered_rules.foreach(println)
        val (_, new_rules) = rules.value.partition(
          {case Array(id, h, b1, b2) => filtered_rules_set.contains((id, h, b1, b2))})
        new_rules
  }

  private def prune_join(preds: Iterable[Int], rules: Array[Array[Int]]) = {
    val predSet =  scala.collection.immutable.HashSet(preds.toList: _*)
    rules.filter({case Array(rid, head, body1, body2) => predSet.contains(body1) && predSet.contains(body2)})
  }
  
  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = {
   // filter to make join dataset size smaller if possible
   val body1_preds = sc.broadcast(rules.map(arr => arr(2)).toSet)
   val body2_preds = sc.broadcast(rules.map(arr => arr(3)).toSet)

   val facts1 = facts.filter({ case Array(q, z, x) => body1_preds.value.contains(q) })
                     .map{ case Array(q, z, x) => (z, (q, x)) }

   val facts2 = facts.filter({ case Array(r, z, y) => body2_preds.value.contains(r) })
                     .map{ case Array(r, z, y) => (z, (r, y)) }
   (facts1, facts2)
  }
}

/* Learner for type4 rule:  h(x, y) <- q(x, z) ^ r(z, y) */
object OpPlusLearnerType4 extends OpPlusLearnerBase {

  protected override def prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = {
    val filtered_rules = h2.map{ case Array(pred, obj) => (obj, pred)}
                           .cogroup(h1.map{ case Array(pred, sub) => (sub, pred) }, NUM_CHECK_TASKS)
                                  .flatMap({ case (objsub, (h2_preds, h1_preds)) => prune_join(h2_preds, h1_preds, rules.value) })
                                  .map({case Array(rid, h, b1, b2) => (rid, h, b1, b2)})
                                  .distinct()

    val filtered_rules_set = filtered_rules.collect().toSet
            // debug 
            // filtered_rules_set.foreach(println)

    val (_, new_rules) = rules.value.partition( { case Array(id, h, b1, b2) => filtered_rules_set.contains((id, h, b1, b2)) })
    new_rules
  }

  private def prune_join(h2_preds: Iterable[Int], h1_preds: Iterable[Int], rules: Array[Array[Int]]) = {
    val h2_predSet =  h2_preds.toSet
    val h1_predSet =  h1_preds.toSet
    rules.filter({case Array(rid, head, body1, body2) => h2_predSet.contains(body1) && h1_predSet.contains(body2)})
  }

  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = {
   // filter to make join dataset size smaller if possible
   val body1_preds = sc.broadcast(rules.map(arr => arr(2)).toSet)
   val body2_preds = sc.broadcast(rules.map(arr => arr(3)).toSet)

   val facts1 = facts.filter({ case Array(q, x, z) => body1_preds.value.contains(q) })
                     .map{ case Array(q, x, z) => (z, (q, x)) }

   val facts2 = facts.filter({ case Array(r, z, y) => body2_preds.value.contains(r) })
                     .map{ case Array(r, z, y) => (z, (r, y)) }
   (facts1, facts2)
  }
}


/*Learner for type 5 rules: h(x, y) <- q(z, x), r(y, z)*/
object OpPlusLearnerType5 extends OpPlusLearnerBase {
  protected override def prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = {
    val filtered_rules = h1.map{ case Array(pred, sub) => (sub, pred) }
                           .cogroup(h2.map{ case Array(pred, obj) => (obj, pred) }, NUM_CHECK_TASKS)
                                  .flatMap({ case (subobj, (h1_preds, h2_preds)) => prune_join(h1_preds, h2_preds, rules.value) })
                                  .map({case Array(rid, h, b1, b2) => (rid, h, b1, b2)})
                                  .distinct()

    val filtered_rules_set = filtered_rules.collect().toSet
            // debug 
            // filtered_rules_set.foreach(println)

    val (_, new_rules) = rules.value.partition( { case Array(id, h, b1, b2) => filtered_rules_set.contains((id, h, b1, b2)) })
    new_rules
  }

  private def prune_join(h1_preds: Iterable[Int], h2_preds: Iterable[Int], rules: Array[Array[Int]]) = {
    val h1_predSet =  h1_preds.toSet
    val h2_predSet =  h2_preds.toSet
    rules.filter({case Array(rid, head, body1, body2) => h1_predSet.contains(body1) && h2_predSet.contains(body2)})
  }

  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = {
   // filter to make join dataset size smaller if possible
   val body1_preds = sc.broadcast(rules.map(arr => arr(2)).toSet)
   val body2_preds = sc.broadcast(rules.map(arr => arr(3)).toSet)

   val facts1 = facts.filter({ case Array(q, z, x) => body1_preds.value.contains(q) })
                     .map{ case Array(q, z, x) => (z, (q, x)) }

   val facts2 = facts.filter({ case Array(r, y, z) => body2_preds.value.contains(r) })
                     .map{ case Array(r, y, z) => (z, (r, y)) }
   (facts1, facts2)
  }
}


/* Learner for type6 rules: h(x, y) <- q(x, z), r(y, z) */
object OpPlusLearnerType6 extends OpPlusLearnerBase {
  protected override def prune(rules: Broadcast[Array[Array[Int]]], h1:RDD[Array[Int]], h2: RDD[Array[Int]]) = {
    val filtered_rules = h2.map{ case Array(pred, obj) => (obj, pred) }
                           .groupByKey(NUM_CHECK_TASKS)
                           .flatMap({ case (obj, (h2_preds)) => prune_join(h2_preds, rules.value) })
                           .map({case Array(rid, h, b1, b2) => (rid, h, b1, b2)})
                           .distinct()

    val filtered_rules_set = filtered_rules.collect().toSet
            // debug 
            // filtered_rules_set.foreach(println)

    val (_, new_rules) = rules.value.partition( { case Array(id, h, b1, b2) => filtered_rules_set.contains((id, h, b1, b2)) })
    new_rules
  }

  private def prune_join(h2_preds: Iterable[Int], rules: Array[Array[Int]]) = {
    val h2_predSet =  h2_preds.toSet
    rules.filter({case Array(rid, head, body1, body2) => h2_predSet.contains(body1) && h2_predSet.contains(body2)})
  }

  protected override def getMappedFacts(sc:SparkContext, facts: RDD[Array[Int]], rules: Array[Array[Int]]) = {
   // filter to make join dataset size smaller if possible
   val body1_preds = sc.broadcast(rules.map(arr => arr(2)).toSet)
   val body2_preds = sc.broadcast(rules.map(arr => arr(3)).toSet)

   val facts1 = facts.filter({ case Array(q, x, z) => body1_preds.value.contains(q) })
                     .map{ case Array(q, x, z) => (z, (q, x)) }

   val facts2 = facts.filter({ case Array(r, y, z) => body2_preds.value.contains(r) })
                     .map{ case Array(r, y, z) => (z, (r, y)) }
   (facts1, facts2)
  }
}