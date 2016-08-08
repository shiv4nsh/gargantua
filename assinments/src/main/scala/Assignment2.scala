/**
  * Created by shivansh on 5/8/16.
  */

import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql._

/**
  * Created by shivansh on 20/7/16.
  */
object Assignment2 extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("assignment2")
    .getOrCreate()

  val sparkContext = spark.sparkContext
  //Answer1
  val fireDepartmentDF: DataFrame = spark.read.option("header", "true").csv("Fire_Department_Calls_for_Service.csv").persist()
  val differentTypeCalls = fireDepartmentDF.select("Call Type").distinct()
  fireDepartmentDF.show
  println(s"""Different Type Of Calls:${differentTypeCalls.count()}""")
  val incidentsCount = fireDepartmentDF.select("Call Type", "Incident Number").groupBy("Call Type").count()
  //Answer 2
  incidentsCount.show

  import org.apache.spark.sql.functions._
  import spark.implicits._

  implicit val encoder = Encoders.DATE

  def stringToDate(date: String, dateFormat: String = "MM/dd/yyyy") = {
    val df = new SimpleDateFormat(dateFormat)
    val date1: util.Date = df.parse(date)
    new java.sql.Date(date1.getTime)
  }

  val callDF: DataFrame = fireDepartmentDF.select("Call Date", "Incident Number", "City", "Neighborhood  District").toDF("call_date", "incident_number", "city", "neighbour")
  val funcDate: (String) => Date = stringToDate(_)
  val stringToDateFunc = udf(funcDate)
  val newtable = callDF.withColumn("call_date", stringToDateFunc(callDF("call_date")))
  newtable.createOrReplaceTempView("table")
  val yearsCount = spark.sql("Select year(call_date) as date from table ").distinct().count()

  //Answer3
  println(s"yearsOfData:$yearsCount")

  val maxDate = spark.sql("Select max(call_date) as maxDate from table").collect()(0).getDate(0)

  def subSevenDays(date: Date): Date = stringToDate(date.toLocalDate.minusWeeks(1).toString, "yyyy-MM-dd")

  //Answer4
  val lastSevenDay = spark.sql(s"Select count(incident_number) as total_incident from table where call_date > to_date('${subSevenDays(maxDate).toString}')")
  val totalCount = lastSevenDay.collect()(0).getLong(0)
  println(s"Total Count: $totalCount")

  //Answer5
  val neighbourhood: Dataset[Row] = newtable.filter("city=='San Francisco'").filter("year(call_date)==2015")
  val newN = neighbourhood.groupBy("neighbour").count().orderBy($"count".desc)
  println(s"Neighbourhood in SF generated most number of calls last year :${newN.first().getString(0)}")

}
