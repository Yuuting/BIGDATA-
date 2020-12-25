import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
object AssociationRuleMiningDemo {
	def main(args: Array[String]): Unit = {
		if (args.length < 2) {
			System.err.println("Usage: AssociationRuleMiningDemo <transactions> <output path>")
			System.exit(1)
		}
		val Array(transactionsFileName, outputPath) = args
			val sparkConf = new SparkConf().setAppName("Association Rule Mining Demo")
			val sc = new SparkContext(sparkConf)
			val transactions = sc.textFile(transactionsFileName, 2).cache
			val transactionSize = transactions.count();
			val itemsets = transactions.map(toList).map(findSortedCombinations(_))
   			.flatMap(x => x).filter(_.size > 0).map(x => (x, 1L)).cache
			val minSup = 0.4
			val combined = itemsets.reduceByKey(_ + _)
   			.map(x => (x._1, (x._2, x._2.toDouble / transactionSize.toDouble)))
   			.filter(_._2._2 >= minSup).cache
			val subitemsets = combined.flatMap(itemset => {
				val list = itemset._1
				val frequency = itemset._2._1
				val support = itemset._2._2
				var result = List((list, (List(""), (frequency, support))))
				if (list.size == 1) {
					result
				} else {
					for (i <- 0 until list.size) {
						val listX = removeOneItem(list, i)
						val listY = list.diff(listX)
						result ++= List((listX, (listY, (frequency, support))))
					}
					result
				}
			}).cache
			val rules = subitemsets.groupByKey()
			val assocRules = rules.map(in => {
				val listX = in._1
				val listYLists = in._2.toList
				val countX = listYLists.filter(_._1(0) == "")(0)
				val newListYLists = listYLists.diff(List(countX))
				if (newListYLists.isEmpty) {
					val result = List((List(""), List(""), 0.0D, 0.0D))
					result
				} else {
					val result = newListYLists.map(t2 => (listX, t2._1, t2._2._1.toDouble 
             	/countX._2._1.toDouble, t2._2._2))
					result
				}
			})
			val minConf = 0.7
			val finalResult = assocRules.flatMap(x => x).filter(_._3 >= minConf)
			finalResult.saveAsTextFile(outputPath)
			System.exit(0)
		}
		def toList(transaction: String): List[String] = {
			val list = transaction.trim().split(",").toList
			list
		}
		def removeOneItem(list: List[String], i: Int): List[String] = {
			if ((list == null) || list.isEmpty) {
				return list
		}
			if ((i < 0) || (i > (list.size - 1))) {
     			return list
			}
			val cloned = list.take(i) ++ list.drop(i + 1)
			cloned
		}
		def findSortedCombinations[T](elements: List[T])(implicit B: Ordering[T]): List[List[T]] = {
			val result = elements.sorted(B).toSet[T].subsets.map(_.toList).toList
			result
		}
	}
