import com.instructure.DataMigration
import org.scalatest.FunSuite
import scala.io.Source

class DataMigrationTest extends FunSuite {

  DataMigration.main(Array())
  val lines = Source.fromFile("target/lessDataMigration/part-00000").getLines.toList

  test("Data migration file save test") {
     //DataMigration.main(Array())
     //val lines = Source.fromFile("target/lessDataMigration/part-00000").getLines.toList
     val expectedLineCount = 5
     assert(expectedLineCount == lines.size)
   }

  test("Data Migration file contains the following request id ") {
    //DataMigration.main(Array())
    //val lines = Source.fromFile("target/lessDataMigration/part-00000").getLines.toList
    assert(lines(0).contains("5625fa4e"))
  }


}