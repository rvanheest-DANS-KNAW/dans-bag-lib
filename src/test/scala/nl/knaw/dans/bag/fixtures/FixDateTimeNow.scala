package nl.knaw.dans.bag.fixtures

import org.joda.time.{ DateTime, DateTimeUtils }
import org.scalatest.BeforeAndAfterEach

trait FixDateTimeNow extends BeforeAndAfterEach {
  this: TestSupportFixture =>

  protected val fixedDateTimeNow: DateTime = new DateTime(2017, 7, 30, 0, 0)

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    DateTimeUtils.setCurrentMillisFixed(fixedDateTimeNow.getMillis)
  }

  override protected def afterEach(): Unit = {
    DateTimeUtils.setCurrentMillisOffset(0L)

    super.afterEach()
  }
}
