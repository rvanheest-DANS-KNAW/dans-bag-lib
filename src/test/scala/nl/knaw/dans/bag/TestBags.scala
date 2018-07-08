package nl.knaw.dans.bag

import better.files.File
import nl.knaw.dans.bag.v0.Bag

import scala.util.{ Failure, Success, Try }

trait TestBags extends FileSystemSupport {
  this: TestSupportFixture =>

  protected val fetchBagDir: File = testDir / "bag-with-fetch"
  protected val multipleKeysBagDir: File = testDir / "multiple-keys-in-baginfo"
  protected val multipleManifestsBagDir: File = testDir / "bag-with-multiple-manifests"
  protected val simpleBagDir: File = testDir / "simple-bag"

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val bags = "/test-bags/simple-bag" -> simpleBagDir ::
      "/test-bags/multiple-keys-in-baginfo" -> multipleKeysBagDir ::
      "/test-bags/bag-with-multiple-manifests" -> multipleManifestsBagDir ::
      "/test-bags/bag-with-fetch" -> fetchBagDir ::
      Nil

    for ((src, target) <- bags)
      File(getClass.getResource(src)).copyTo(target)
  }

  protected implicit def removeTry(bag: Try[Bag]): Bag = bag match {
    case Success(x) => x
    case Failure(e) => throw e
  }

  protected def fetchBag(): Bag = Bag.read(fetchBagDir)

  protected def multipleKeysBag(): Bag = Bag.read(multipleKeysBagDir)

  protected def multipleManifestsBag(): Bag = Bag.read(multipleManifestsBagDir)

  protected def simpleBag(): Bag = Bag.read(simpleBagDir)
}
