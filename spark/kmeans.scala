//设置质心数量
val k = 2
//设置质心距离阈值
val e = 0.1
val maxIterations = 5
var iteration = 0
//使用sparkContext类的textFile方法读取输入文件，
//并将每一行的坐标值用空格分隔，最后将坐标值转化为Double数字类型以便后续的距离计算
val data = sc.textFile("/public/input.csv").map(x => x.split(" ").map(_.toDouble))
data.cache
var centers: Array[Array[Double]] = _

//从data数据里随机选取两个点作为初始质心
do {
  centers = data.takeSample(true, 2, System.nanoTime.toInt)
} while (centers.map(_.deep).toSet.size != k)

//计算欧式距离
def euclideanDistance(xs: Array[Double], ys: Array[Double]) = {
  Math.sqrt((xs zip ys).map {
    case (x, y) => Math.pow(y - x, 2)
  }.sum)
}

var changed = true
val dims = centers(0).length

while (changed && iteration < maxIterations) {
  iteration += 1
  changed = false
  //计算每个点距离最近的质心
  val pointWithClass = data.map({ point =>
    val closestCenterIndex = centers.zipWithIndex.map({
      case (center, index) => {
        val distance = euclideanDistance(point, center)
        (distance, index)
      }
    }).reduce((d1, d2) => if (d1._1 > d2._1) d2 else d1)._2
    (closestCenterIndex, (point, 1))
  })
  //计算每个质心所属点的坐标总和还有点的个数
  val totalContribs = pointWithClass.reduceByKey({
    case ((xs, c1), (ys, c2)) =>
      ((xs zip ys).map { case (x, y) => x + y }, c1 + c2)
  }).collect
  //计算新的质心
  val newCenters = totalContribs.map {
    case (centerIndex, (sum, counts)) =>
      (centerIndex, sum.map(_ / counts))
  }.sortBy(_._1).map(_._2)
  for (i <- 0 until k) {
    //如果新质心和上次选取的质心之间的距离大于阈值，则继续迭代
    if (euclideanDistance(centers(i), newCenters(i)) > e) {
      changed = true;
      centers(i) = newCenters(i)
    }
  }
}    

centers.foreach(x=>println(x.mkString(",")))