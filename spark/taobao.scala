import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import java.util.Properties
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val train_data = sc.textFile("/user/stu2020110047/taobao/train_after.csv")
val test_data = sc.textFile("/user/stu2020110047/taobao/test_after.csv")

val train= train_data.map{line =>
  val parts = line.split(',')
  LabeledPoint(parts(4).toDouble,Vectors.dense(parts(1).toDouble,parts
(2).toDouble,parts(3).toDouble))
}
val test = test_data.map{line =>
  val parts = line.split(',')
  LabeledPoint(parts(4).toDouble,Vectors.dense(parts(1).toDouble,parts(2).toDouble,parts(3).toDouble))
}

val numIterations = 20
val model = SVMWithSGD.train(train, numIterations)

model.clearThreshold()
val scoreAndLabels = test.map{point =>
  val score = model.predict(point.features)
  score+" "+point.label
}
scoreAndLabels.collect().foreach(println)

model.setThreshold(0.0)
scoreAndLabels.collect().foreach(println)