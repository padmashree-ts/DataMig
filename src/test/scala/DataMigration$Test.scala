import com.instructure.DataMigration
import org.apache.spark.{SparkContext, SparkConf}
import org.instructure.sparktestutil.SparkTestUtils
import org.scalatest.{ShouldMatchers, BeforeAndAfter, FunSuite}

/**
 * Created by pteeka on 6/8/15.
 */
class DataMigration$Test extends SparkTestUtils with ShouldMatchers with BeforeAndAfter {

  sparkTest("test test") {
    DataMigration.run(sc,"/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt","/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration" )
  }

}
