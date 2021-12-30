
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkConf
  import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral

  object Spark_ass_2 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:/null")
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "spark_df")
    sparkConf.set("spark.master", "local[2]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()



    val first_rdd = spark.sparkContext.textFile("C:/Users/Admin/IdeaProjects/spark_Assignment2/src/main/resources/logs_data.txt")
    val count_rdd = first_rdd.count()
    println("====How many lines does the RDD contain=====")
    count_rdd.foreach(println)


println("====Counting the number of WARNing messages =====")

    val warn_count = first_rdd.map(x => {
      val fields = x.split(",")
      val log = fields(0)
      (log)

    })
    val r1 = warn_count.map(x => x).filter(x => x == "WARN").map(x => (x, 1))
    val r2 = r1.reduceByKey((x, y) => x + y).foreach(println)

println("====How many repositories where processed in total====")

    val repo = first_rdd.flatMap(x => x.split(" "))
      .filter(x => x == "api_client.rb:")
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(println)

    println("======Which client did most FAILED HTTP requests======")

    val repo1 = first_rdd.flatMap(x => x.split(","))
      .filter(x => x.contains("URL") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).sortBy(x => x._2)
      .foreach(println)

    println("====Which client did most FAILED HTTP requests====")
    val rdd1 = first_rdd.flatMap(x => x.split(","))
      .filter(x => x.contains("Failed") && x.contains("ghtorrent-"))
      .map(x => (x.substring(0, 13), 1))
      .reduceByKey((x, y) => x + y).foreach(println)

    println("=====What is the most active hour of day======")

    val rdd = first_rdd.flatMap(x => x.split(","))
      .filter(x => x.contains("+00"))
      .map(x => (x.substring(0, 11), (x.substring(12,14), 1)))
      .reduceByKey((x, y) => (x._1, x._2 + y._2))
      .foreach(println)

println("====What is the most active repository=====")
    val reqfield = first_rdd.flatMap(x => x.split(","))
      .filter(x => x.contains("ghtorrent.rb: Repo") && x.contains("exists"))
      .map(x => (x.substring(x.indexOf("Repo")+5,x.indexOf("exists")-1),1))
      .reduceByKey((x,y) => x+y).sortBy(x => x._2,false).take(10)
      .foreach(println)



  }

