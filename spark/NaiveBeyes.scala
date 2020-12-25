import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}
object NaiveBeyes {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Naive Bayes")
    val sc = new SparkContext(conf)
    val file = sc.textFile("inputFile1.csv", 2)
    val pair = file.flatMap(line=>{
       val tokens:Array[String]=line.split(" ")
       val classificationIndex=tokens.length-1
       val theClassification:String=tokens(classificationIndex)
       var result =new Array[(String,String)](classificationIndex+1)
       var i=0
       while(i < classificationIndex){
         result(i)=((tokens(i),theClassification))
         i+=1
       }
       result(i)=("CLASS",theClassification)
       result
     }
     ).map(pair => (pair, 1))
     val counts=pair.reduceByKey(_+_)
     val countsAsMap=counts.collectAsMap()
     var CLASSIFICATION=new ArrayBuffer[String]()
     var PT=new HashMap[(String,String),Double]
     for((key,value)<-countsAsMap){
       val classification=key._2
       if(key._1=="CLASS"){
         CLASSIFICATION+=classification
         PT(key)=value
       }
       else{
         val sum = countsAsMap(("CLASS",key._2))
         if(value==null){
           PT(key)=0.0
         }
         else{
           PT(key)=value.toDouble/sum.toDouble
         }
       }
     }
     var trainingSize=0.0
     for(classification<-CLASSIFICATION){
       trainingSize+=PT(("CLASS",classification))
     }
     for(classification<-CLASSIFICATION){
       PT(("CLASS",classification))/=trainingSize
     }
     val PT2save=PT.toArray
     val ptRDD = sc.parallelize(PT2save, 2)
     ptRDD.saveAsTextFile("PT")
     val CLFRDD=sc.parallelize(CLASSIFICATION, 1)
     CLFRDD.saveAsTextFile("CLASSIFICATION")
     val testdata = sc.textFile("inputFile2.csv", 1)
     val broadcastPT=sc.broadcast(PT)
     val broadcastCLASS=sc.broadcast(CLASSIFICATION)
     val classified=testdata.map(line=>{
       val attributes:Array[String]=line.split(" ")
       val PT=broadcastPT.value
       val CLASS=broadcastCLASS.value
       var selectedCLASS:String=null
       var maxProbility:Double=0
       for(aCLASS<-CLASS){
         var postprob:Double=PT(("CLASS",aCLASS))
         for(i<-0 until attributes.length){
           var prob:Double=0.0
           if(PT.contains((attributes(i),aCLASS))){
             prob=PT((attributes(i),aCLASS))
             println("P("+attributes(i)+"|"+aCLASS+") "+prob)
           }
           postprob*=prob
         }
         if(selectedCLASS==null){
           selectedCLASS=aCLASS
           maxProbility=postprob
         }
         if(postprob>maxProbility){
           selectedCLASS=aCLASS
           maxProbility=postprob
         }
       }
       (line,selectedCLASS)
     })
     classified.map(x=>x._1+" "+x._2)saveAsTextFile("outputFile")
   }
	}
