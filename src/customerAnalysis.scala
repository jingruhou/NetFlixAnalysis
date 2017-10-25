import java.io.{File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-10-22.
  */

case class training1(CustomerID:String, Rating:String, Date:String)
case class movieTitles1(MovieID:String,YearOfRelease:String,Title:String)

object customerAnalysis {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境配置
      */
    val conf = new SparkConf().setAppName("customerAnalysis").setMaster("local[32]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    /**
      * 数据加载
      */
    val trainingData = sc.textFile("Resource/Data/training_set/training_set/mv*.txt").cache()
    val movieTitlesData = sc.textFile("Resource/Data/movie_titles.txt")

    trainingData.take(5).foreach(println)

    movieTitlesData.take(5).foreach(println)

    // 包含“：”的为首行
    def isHeader(line:String) = line.contains(":")
    /**
      * 统计所有用户的平均评分和评分数
	    * a)计算每一个电影(MovieID)文件中,构建元组:(CustomerID,Rating)
	    * b)count每一个用户的电影评分数量[对多少电影有过评分]
	    * c)聚合计算reduceByKey(),其中Key为CustomerID,结果为(CustomerID,Ratings[所有])
	    * d)计算用户对电影的平均评分
      *
      */

    val writer2 = new PrintWriter(new File("Resource/result2.csv"))

    val trainingDataDF = sc.textFile("Resource/Data/training_set/training_set/mv*.txt")
      .filter(x => !isHeader(x))
      .map(_.split(","))
      .map(p => training1(p(0),p(1),p(2))).toDF()

    trainingDataDF.registerTempTable("trainingDataTable")

    val result2 = trainingDataDF.groupBy("CustomerID").agg(("Rating","sum"),("Rating","avg"))
    result2.show(100)

    println(result2)

    writer2.close()


    /**
      * 停止SC对象
      */
    sc.stop()

  }
}
