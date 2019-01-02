import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.bson.Document
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner

case class explicitClass(spark: Int)

@RunWith(classOf[JUnitRunner])
class TestMongoDBConnector extends FunSuite with BeforeAndAfterAll {

  var conf: org.apache.spark.SparkConf = _
  var sc: SparkContext = _
  var spark: SparkSession = _
  var writeConfig: WriteConfig = _
  var readConfig: ReadConfig = _

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

  test("Write RDD") {

    val sparkDocuments = sc.parallelize((1 to 10).map(i => Document.parse(s"{spark: $i}")))
    MongoSpark.save(sparkDocuments, writeConfig)
  }


  test("Write RDD 2") {

    val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test: $i}")))
    documents.saveToMongoDB() // Uses the SparkConf for configuration

  }

  test("Unsupported Types") {

    import scala.collection.JavaConverters._

    val documents = sc.parallelize(
      Seq(new Document("fruits", List("apples", "oranges", "pears").asJava))
    )
    MongoSpark.save(documents)

  }

  test("Read to RDD") {

    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)

  }

  test("Read helper methods") {

    val rdd = sc.loadFromMongoDB() // Uses the SparkConf for configuration
    rdd.take(5).foreach(println)

  }

  test("Filter") {

    val rdd = MongoSpark.load(sc, readConfig)
    val filteredRdd = rdd.filter(doc => doc.getInteger("test") < 5)
    println(filteredRdd.count)
    println(filteredRdd.first.toJson)

  }

  test("Aggregation") {

    val rdd = MongoSpark.load(sc, readConfig)
    val aggregatedRdd = rdd.withPipeline(
      Seq(Document.parse("{ $match: { spark : { $gt : 5 } } }")))

    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)

  }

  test("DF filter") {

    val sparkSession = SparkSession.builder().getOrCreate()
    val df = sparkSession.loadFromMongoDB()

    df.filter(df("spark") < 5).show()
  }


  test("SQL") {

    val sparkSession = SparkSession.builder().getOrCreate()
    val characters = MongoSpark.load[explicitClass](sparkSession)
    characters.createOrReplaceTempView("spark")

    val centenarians = sparkSession.sql("SELECT * FROM spark WHERE spark > 1")
    centenarians.show()

  }
}