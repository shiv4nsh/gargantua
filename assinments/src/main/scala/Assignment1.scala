import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shivansh on 20/7/16.
  */
object Assignment1 extends App {

  val conf = new SparkConf().setAppName("assignment1").setMaster("local")
  val sc = new SparkContext(conf)
  //Question1
  val pagecounts = sc.textFile("/home/shivansh/Documents/pagecounts-20151201-220000")
  pagecounts.persist(StorageLevel.MEMORY_AND_DISK)
  val tenRecords = pagecounts.take(10)
  println(s"tenRecords:${tenRecords.mkString(",")}")
  val count = pagecounts.count()
  println(s"count:$count")
  val lineRDD = pagecounts.map(_.split(" "))
  val englishPages = lineRDD.filter(a => a(0).toLowerCase == "en")
  val englishPagesCount = englishPages.count
  println(s"englishPageCount:$englishPagesCount")
  val hitsPairRDD = lineRDD.map(a => (a(1), a(2).toDouble)).groupByKey().mapValues(a => a.reduce(_ + _)).filter(a => a._2 > 200000)
  println(s"hitsPairRDD:${hitsPairRDD.collect.mkString(",")}")
}
