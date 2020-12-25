/*
读取输入文件trainSet.csv，然后调用map算子将每一条记录用空格分割，
最终map算子将trainSet.csv中的每一行差分成6列值存入trainSet中
 */
val trainSet = sc.textFile("/public/trainSet.csv").map(line => {
  val datas = line.split(" ")
  (datas(0), datas(1), datas(2), datas(3), datas(4), datas(5))
})

//调用broadcast将trainSet广播到各个计算节点
val bcTrainSet = sc.broadcast(trainSet.collect())

//设置kNN算法中的k值并将其广播到各个计算节点
var bcK = sc.broadcast(3)

//使用textFile读取输入文件testSet.csv存入testSet中
val testSet = sc.textFile("/public/testSet.csv")

val resultSet = testSet.map(line => {
  val datas = line.split(" ")
  val x = datas(0).toDouble
  val y = datas(1).toDouble
  val z = datas(2).toDouble
  val t = datas(3).toDouble
  val s = datas(4).toDouble
  val trainDatas = bcTrainSet.value
  var set = Set[Tuple7[Double, Double, Double, Double, Double, Double, String]]()
  //计算每一条记录与trainSet中各个记录的距离
  trainDatas.foreach(trainData => {
    val tx = trainData._2.toDouble
    val ty = trainData._3.toDouble
    val tz = trainData._4.toDouble
    val tt = trainData._5.toDouble
    val ts = trainData._6.toDouble
    val distance = Math.sqrt(Math.pow(x - tx, 2) + Math.pow(y - ty, 2) + Math.pow(z - tz, 2) + Math.pow(t - tt, 2) + Math.pow(s - ts, 2))
    set += Tuple7(x, y, z, t, s, distance, trainData._1)
  })
  val list = set.toList
  //将计算的距离进行排序
  val sortList = list.sortBy(item => item._6)
  var categoryCountMap = Map[String, Int]()
  val k = bcK.value
  //根据设置的k值选取与该记录距离最小的前k个已知分类记录
  for (i <- 0 to (k - 1)) {
    val category = sortList(i)._7
    val count = categoryCountMap.getOrElse(category, 0) + 1
    categoryCountMap += (category -> count)
  }
  var rCategory = ""
  var maxCount = 0
  //取最大值
  categoryCountMap.foreach(item => {
    if (item._2.toInt > maxCount) {
      maxCount = item._2.toInt
      rCategory = item._1
    }
  })
  ("Test sample", x, y, x, t, s, "Test result", rCategory)
})

resultSet.saveAsTextFile("testSetResult") 