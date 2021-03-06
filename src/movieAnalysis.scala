import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import java.io.File

/**
  * Created by hjr on 17-10-22.
  */

case class training(CustomerID:String, Rating:String, Date:String)
case class movieTitles(MovieID:String,YearOfRelease:String,Title:String)

object movieAnalysis {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境配置
      */
    val conf = new SparkConf().setAppName("movieAnalysis_xunfang.com").setMaster("local[32]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    /**
      * 数据准备：
      *
      * (1)去除training_set的所有首行信息
      * (2)将原始数据转化为DataFrame
      * (3)将DataFrame注册为SparkSQL的临时表
      *
      * training(CustomerID:String, Rating:String, Date:String)
      *
      * movieTitles(MovieID:String,YearOfRelease:String,Title:String)
      *
      */
    // 包含“：”的为首行
    def isHeader(line:String) = line.contains(":")
    val trainingDataDF = sc.textFile("Resource/Data/training_set/training_set/mv*.txt")
      .filter(x => !isHeader(x))
      .map(_.split(","))
      .map(p => training(p(0),p(1),p(2))).toDF()

    trainingDataDF.registerTempTable("trainingDataTable")

    val movieTitlesDataDF = sc.textFile("Resource/Data/movie_titles.txt")
      .map(_.split(","))
      .map(p => movieTitles(p(0),p(1),p(2)))
      .toDF()

    movieTitlesDataDF.registerTempTable("movieTitlesDataTable")

    /**
      * 需求1： 统计所有2000年以后上映影片的平均评分和评分数
      *
      * a)从表movie_titles.txt找到YearOfRelease>2000的电影MovieID;
	    * b)统计总共有多少电影count;
	    * c)在training_set里面查找a)中的MovieID
	    * d)针对每一个MovieID,就对该MovieID评分的用户CustomerID进行统计计数,
      *   并对评分数据Rating求和(评分数)
	    * e)计算每一个MovieID的平均评分=(评分数)/(所有评分用户总数)
      *
      */
    val movieIDs2000 = movieTitlesDataDF.filter(movieTitlesDataDF("YearOfRelease") > 2000)
    movieIDs2000.take(20).foreach(println)
    println("2000年以后上映的影片数量: "+movieIDs2000.count())

    val fileName2001 = "Resource/Data/training_set/training_set2000/training_set/mv_0002001.txt"
    val movieID2001 = sc.textFile(fileName2001)

    val movie2001Data = movieID2001.filter(x => !isHeader(x)).map(_.split(",")).map(p => training(p(0),p(1),p(2))).toDF()
    movie2001Data.registerTempTable("movie2001DataTable")

    movie2001Data.groupBy("CustomerID").count().show(100) //说明该数据中没有用户对一个电影评分两次以上
    val customersCount = movie2001Data.count()

    val ratingsCount = movie2001Data.agg("Rating" -> "sum")
    val avgMovie2001 = movie2001Data.agg("Rating" -> "avg")
    avgMovie2001.show()

    /*******************************************************************************************************************/

    val writer1 = new PrintWriter(new File("Resource/avgRatingAndCount.txt"))
    /**
      * 求平均评分与评分数 方法定义
      * @param fileNumbers 训练数据文件数字编号
      *
      **/

    def avgRatingAndCount(fileNumbers:BigInt): Unit ={

      var fileNameNumber = ""

      if(fileNumbers < 10000)
        //文件名小于等于10000的处理
        fileNameNumber = "mv_"+"000"+fileNumbers+".txt"
      else
      //文件名大于10000的处理
        fileNameNumber = "mv_"+"00"+fileNumbers+".txt"

      val fileName = "Resource/Data/training_set/training_set2000/training_set/"+fileNameNumber
      val movieID = sc.textFile(fileName).cache()

      val movieData = movieID.filter(x => !isHeader(x)).map(_.split(",")).map(p => training(p(0),p(1),p(2))).toDF()
      movieData.registerTempTable("movieDataTable")

      //movieData.groupBy("CustomerID").count().show(10) //说明该数据中没有用户对一个电影评分两次以上
      val customersCount = movieData.count()

      val ratingsCount = movieData.agg("Rating" -> "sum").collect()
      val avgMovie = movieData.agg("Rating" -> "avg").collect()

      //val avgRating = avgMovie.zip(ratingsCount)
      //avgRating.foreach(println)

      val result = fileNumbers+"\t"+avgMovie(0)+"\t"+ratingsCount(0)+"\t"+customersCount
      writer1.println(result)
      //存储计算结果(平均评分 评分数)
      println("计算结果： "+result)
    }

    /**
      *  *
      * 循环遍历所有训练数据文件[2001-17770]
      *
      * Note:组装文件名
      */

    for(fileNumbers <- 2001 to 17770){
      avgRatingAndCount(fileNumbers)
    }
    writer1.close()

  /********************************************************************************************************************/


    /**
      * 需求2： 得到平均评分前5的影片的所有评分
      *
      * a)计算每一个电影(MovieID)文件中,构建元组:(CustomerID,Rating)
	    * b)count每一个用户的电影评分数量[对多少电影有过评分]
	    * c)聚合计算reduceByKey(),其中Key为CustomerID,结果为(CustomerID,Ratings[所有])
	    * d)计算用户对电影的平均评分
      *
      *
      * 2001	[3.3146025501899077]	[97741.0]	29488
      * 2002	[2.2596153846153846]	[470.0]	208
      * 2003	[3.022222222222222]	[544.0]	180
      * 2004	[2.842249657064472]	[2072.0]	729
      * 2005	[2.7083333333333335]	[260.0]	96
      * 2006	[2.432981316003249]	[2995.0]	1231
      *
      */

    //val writer2 = new PrintWriter(new File("Resource/avgRatingAndCountSorted.txt"))

   /**
     * 加载CSV文件
     *  val avgRatingAndCountData = sc.textFile("Resource/avgRatingAndCount.csv")
     *  val avgRatingAndCountData = sqlContext.read.format("com.databricks.spark.csv")
     *  .option("header","false")
     *  .option("inferSchema",true.toString)
     *  .load("Resource/avgRatingAndCount.csv")
     *
     */
   val avgRatingAndCountData = sc.textFile("Resource/avgRatingAndCount.txt")

    avgRatingAndCountData.take(10).foreach(println)



    val line2Tuple4 = avgRatingAndCountData.map(line =>{
      val lineData = line.split("\n")
      val lineTuple = lineData(0).split("\t")
      (lineTuple(0),lineTuple(1),lineTuple(2),lineTuple(3))
    })

    val result2 = line2Tuple4.map(x => ((x._2),x)).sortByKey().values


    result2.repartition(1).saveAsTextFile("Resource/avgRatingAndCountSorted")

    /**
      * 停止SC对象
      */
    sc.stop()
  }
}
