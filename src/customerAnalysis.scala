import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hjr on 17-10-22.
  */
object customerAnalysis {
  def main(args: Array[String]): Unit = {
    /**
      * 初始化环境配置
      */
    val conf = new SparkConf().setAppName("customerAnalysis").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /**
      * 数据加载
      */
    val trainingData = sc.textFile("Resource/Data/training_set/training_set/mv*.txt")
    val movieTitlesData = sc.textFile("Resource/Data/movie_titles.txt")

    trainingData.take(5).foreach(println)

    movieTitlesData.take(5).foreach(println)

    /**
      * 停止SC对象
      */
    sc.stop()

  }
}
