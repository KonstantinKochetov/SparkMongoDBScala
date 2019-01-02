import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestMongoDBStreaming extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var spark: SparkSession = _
  var writeConfig: WriteConfig = _
  var readConfig: ReadConfig = _

  case class WordCount(word: String, count: Int)

  override protected def beforeAll() {

    conf = new SparkConf().setMaster("local[4]").setAppName("TestMongoDBConnector")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.mongodbconnector")
    conf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.mongodbconnector")

    sc = new SparkContext(conf)

    writeConfig = WriteConfig(
      Map("collection" -> "spark", "writeConcern.w" -> "majority"),
      Some(WriteConfig(sc)))

    this.readConfig = ReadConfig(
      Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"),
      Some(ReadConfig(sc)))
  }

  test("Streaming") {

    // run: nc -lk 9999. Send text input through command line
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val ssc = new StreamingContext(sc, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.foreachRDD({ rdd =>
      val wordCounts = rdd.map({ case (word: String, count: Int)
      => WordCount(word, count) }).toDF()
      wordCounts.write.mode("append").mongo()
    })

    ssc.start()
  }

}