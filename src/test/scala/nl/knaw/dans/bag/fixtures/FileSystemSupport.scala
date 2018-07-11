package nl.knaw.dans.bag.fixtures

import better.files.File
import better.files.File.currentWorkingDirectory
import org.scalatest.BeforeAndAfterEach

trait FileSystemSupport extends BeforeAndAfterEach {
  this: TestSupportFixture =>

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    if (testDir.exists) testDir.delete()
    testDir.createDirectories()
  }

  lazy val testDir: File = currentWorkingDirectory / "target" / "test" / getClass.getSimpleName
}
