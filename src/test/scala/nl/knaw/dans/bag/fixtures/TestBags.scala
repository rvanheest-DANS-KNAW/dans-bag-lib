package nl.knaw.dans.bag.fixtures

import better.files.File
import nl.knaw.dans.bag.{ IBag, v0 }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

trait TestBags extends FileSystemSupport {
  this: TestSupportFixture =>

  protected val fetchBagDirV0: File = testDir / "bag-with-fetch"
  protected val multipleKeysBagDirV0: File = testDir / "multiple-keys-in-baginfo"
  protected val multipleManifestsBagDirV0: File = testDir / "bag-with-multiple-manifests"
  protected val simpleBagDirV0: File = testDir / "simple-bag"

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val bags = "/test-bags/v0/simple-bag" -> simpleBagDirV0 ::
      "/test-bags/v0/multiple-keys-in-baginfo" -> multipleKeysBagDirV0 ::
      "/test-bags/v0/bag-with-multiple-manifests" -> multipleManifestsBagDirV0 ::
      "/test-bags/v0/bag-with-fetch" -> fetchBagDirV0 ::
      Nil

    for ((src, target) <- bags)
      File(getClass.getResource(src)).copyTo(target)
  }

  protected implicit def removeTryV0(bag: Try[IBag]): v0.Bag = bag match {
    case Success(x: v0.Bag) => x
    case Success(b) => fail(s"bag ${ b.baseDir.name } is not a v0 bag")
    case Failure(e) => throw e
  }

  protected def fetchBagV0(): v0.Bag = IBag.read(fetchBagDirV0)

  protected def multipleKeysBagV0(): v0.Bag = IBag.read(multipleKeysBagDirV0)

  protected def multipleManifestsBagV0(): v0.Bag = IBag.read(multipleManifestsBagDirV0)

  protected def simpleBagV0(): v0.Bag = IBag.read(simpleBagDirV0)
}
