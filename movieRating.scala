import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast // Import the broadcast function

object MovieRating extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("my application")
    .master("local[2]")
    .getOrCreate()

  val ratingRdd = spark.sparkContext.textFile("C:/Users/pc/Downloads/ratings.dat")

  case class Rating(userId: Int, movieId: Int, rating: Int, timeStamp: String)

  val ratingTrans = ratingRdd.map(x => x.split("::")).map(x => Rating(x(0).toInt, x(1).toInt, x(2).toInt, x(3)))

  import spark.implicits._
  val ratingDf = ratingTrans.toDF()

  val movieRdd = spark.sparkContext.textFile("C:/Users/pc/Downloads/movies.dat")

  case class Movie(movieId: Int, movieName: String, Genre: String)

  val movieTrans = movieRdd.map(x => x.split("::")).map(x => Movie(x(0).toInt, x(1), x(2)))

  val movieDf = movieTrans.toDF().select("movieId", "movieName")

  ratingDf.createOrReplaceTempView("ratingTable")

  val dff = spark.sql("""select movieId, count(movieId) as movieViewCount, avg(rating) as avgMovieRating 
    from ratingTable 
    group by movieId 
    HAVING COUNT(movieId) > 1000 AND AVG(rating) > 4.0 """)

  // Enable broadcast join if movieDf is small enough
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", " -1") // Adjust the threshold as needed

  val joinType = "inner"
  val joinCondition = dff("movieId") === movieDf("movieId") // Use .col to access columns

  val joinTable = dff.join(broadcast(movieDf), joinCondition, joinType).drop(dff.col("movieId"))

  val finalResult=joinTable.drop("movieViewCount","movieId","avgMovieRating")

  // Show the result or perform other actions as needed
  finalResult.show(false)

  
}
