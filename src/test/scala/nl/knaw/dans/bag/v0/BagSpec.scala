package nl.knaw.dans.bag.v0

import java.io.IOException
import java.net._
import java.nio.charset.StandardCharsets
import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException }
import java.util.UUID

import better.files.File
import gov.loc.repository.bagit.conformance.{ BagLinter, BagitWarning }
import gov.loc.repository.bagit.domain.Version
import gov.loc.repository.bagit.verify.BagVerifier
import nl.knaw.dans.bag.ChecksumAlgorithm.ChecksumAlgorithm
import nl.knaw.dans.bag.IBag.bagAsFile
import nl.knaw.dans.bag._
import nl.knaw.dans.bag.fixtures._
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{ DateTime, DateTimeZone }

import scala.collection.JavaConverters._
import scala.language.{ existentials, implicitConversions, postfixOps }
import scala.util.{ Failure, Success, Try }

class BagSpec extends TestSupportFixture
  with FileSystemSupport
  with TestBags
  with BagMatchers
  with Lipsum
  with FetchFileMetadata
  with CanConnect {

  "create empty" should "create a bag on the file system with an empty data directory" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    baseDir.toJava shouldNot exist

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.toJava should exist
    (baseDir / 'data).toJava should exist
    (baseDir / 'data).listRecursively.toList shouldBe empty
  }

  it should "create parent directories if they do not yet exist" in {
    val baseDir = testDir / "path" / "to" / "an" / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    baseDir.toJava shouldNot exist

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.toJava should exist
  }

  it should "create a bag-info.txt file if the given Map is empty, " +
    "containing only the Payload-Oxum and Bagging-Date" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)

    Bag.empty(baseDir, algorithms) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("0.0"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date()))
    )
  }

  it should "create a bag-info.txt file if the given Map is not empty" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val isVersionOf = s"urn:uuid:${ UUID.randomUUID() }"
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(isVersionOf)
    )

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("0.0"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date())),
      "Is-Version-Of" -> Seq(isVersionOf)
    )
  }

  it should "merge entries in bag-info.txt that have the same key" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val isVersionOf1 = s"urn:uuid:${ UUID.randomUUID() }"
    val isVersionOf2 = s"urn:uuid:${ UUID.randomUUID() }"
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(isVersionOf1, isVersionOf2)
    )

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("0.0"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date())),
      "Is-Version-Of" -> Seq(isVersionOf1, isVersionOf2)
    )
  }

  it should "create empty payload manifests in the bag for all given algorithms" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA512)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.glob("manifest-*.txt").map(_.name).toList should contain only(
      "manifest-sha1.txt",
      "manifest-sha512.txt",
    )

    forEvery(baseDir.glob("manifest-*.txt").toList)(manifest => {
      manifest.contentAsString shouldBe empty
    })
  }

  it should "create tag manifests in the bag for all given algorithms, " +
    "all containing bagit.txt, bag-info.txt and manifest-<alg>.txt checksums" in {
    val baseDir = testDir / "path" / "to" / "an" / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA512)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.glob("tagmanifest-*.txt").map(_.name).toList should contain only(
      "tagmanifest-sha1.txt",
      "tagmanifest-sha512.txt",
    )

    forEvery(algorithms)(algorithm => {
      baseDir should containInTagManifestFileOnly(algorithm)(
        baseDir / "bagit.txt",
        baseDir / "bag-info.txt",
        baseDir / "manifest-sha1.txt",
        baseDir / "manifest-sha512.txt"
      )
    })
  }

  it should "fail when the base directory for the bag-to-be-created already exists" in {
    val baseDir = testDir / "emptyTestBag" createDirectories()
    baseDir / "x.txt" createIfNotExists() writeText lipsum(2)
    baseDir.toJava should exist

    val algorithms = Set(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA512)

    Bag.empty(baseDir, algorithms) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == baseDir.toString =>
    }
  }

  it should "fail when no algorithms are provided" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set.empty[ChecksumAlgorithm]

    Bag.empty(baseDir, algorithms) should matchPattern {
      case Failure(_: IllegalArgumentException) =>
    }
  }

  def createDataDir(): (File, File, File) = {
    val baseDir = testDir / "dataDir" createDirectories()
    val file1 = baseDir / "x" createIfNotExists() writeText lipsum(1)
    val file2 = baseDir / "sub" / "y" createIfNotExists (createParents = true) writeText lipsum(2)

    (baseDir, file1, file2)
  }

  "create from data" should "create a bag on the file system in the place where the payload " +
    "directory was; the payload files should now be located inside the data/ directory" in {
    val (baseDir, file1, file2) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    baseDir.toJava should exist
    baseDir.listRecursively.filter(_.isRegularFile).toList should contain only(file1, file2)

    Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    val file1Data = baseDir / 'data / baseDir.relativize(file1).toString
    val file2Data = baseDir / 'data / baseDir.relativize(file2).toString

    baseDir.toJava should exist
    (baseDir / 'data).toJava should exist
    (baseDir / 'data).listRecursively.filter(_.isRegularFile).toList should contain only(file1Data, file2Data)
    baseDir.list.filter(_.isRegularFile).toList should contain only(
      baseDir / "bagit.txt",
      baseDir / "bag-info.txt",
      baseDir / "manifest-sha1.txt",
      baseDir / "tagmanifest-sha1.txt"
    )
  }

  it should "create a bag-info.txt file if the given Map is empty, " +
    "containing only the Payload-Oxum and Bagging-Date" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1)

    Bag.createFromData(baseDir, algorithms) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("1471.2"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date()))
    )
  }

  it should "create a bag-info.txt file if the given Map is not empty" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val isVersionOf = s"urn:uuid:${ UUID.randomUUID() }"
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(isVersionOf)
    )

    Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("1471.2"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date())),
      "Is-Version-Of" -> Seq(isVersionOf)
    )
  }

  it should "merge entries in bag-info.txt that have the same key" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val isVersionOf1 = s"urn:uuid:${ UUID.randomUUID() }"
    val isVersionOf2 = s"urn:uuid:${ UUID.randomUUID() }"
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(isVersionOf1, isVersionOf2)
    )

    Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    (baseDir / "bag-info.txt").toJava should exist

    baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("1471.2"),
      "Bagging-Date" -> Seq(DateTime.now().toString(ISODateTimeFormat.date())),
      "Is-Version-Of" -> Seq(isVersionOf1, isVersionOf2)
    )
  }

  it should "create manifests in the bag for all given algorithms, " +
    "all containing all payload files and their checksums" in {
    val (baseDir, file1, file2) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA512)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.glob("manifest-*.txt").map(_.name).toList should contain only(
      "manifest-sha1.txt",
      "manifest-sha512.txt",
    )

    forEvery(algorithms)(algorithm => {
      baseDir should containInPayloadManifestFileOnly(algorithm)(
        baseDir / 'data / baseDir.relativize(file1).toString,
        baseDir / 'data / baseDir.relativize(file2).toString
      )
    })
  }

  it should "create tag manifests in the bag for all given algorithms, all containing bagit.txt, " +
    "bag-info.txt and manifest-<alg>.txt checksums" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA512)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.glob("tagmanifest-*.txt").map(_.name).toList should contain only(
      "tagmanifest-sha1.txt",
      "tagmanifest-sha512.txt",
    )

    forEvery(algorithms)(algorithm => {
      baseDir should containInTagManifestFileOnly(algorithm)(
        baseDir / "bagit.txt",
        baseDir / "bag-info.txt",
        baseDir / "manifest-sha1.txt",
        baseDir / "manifest-sha512.txt"
      )
    })
  }

  it should "fail when the payload directory from the bag-to-be-created does not yet exist" in {
    val baseDir = testDir / "non-existing-dir"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    Bag.createFromData(baseDir, algorithms, bagInfo) should matchPattern {
      case Failure(e: NoSuchFileException) if baseDir.toString == e.getMessage =>
    }
  }

  it should "fail when no algorithms are provided" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set.empty[ChecksumAlgorithm]

    Bag.createFromData(baseDir, algorithms) should matchPattern {
      case Failure(_: IllegalArgumentException) =>
    }
  }

  "read bag" should "load a bag located in the given directory into the object structure for a Bag" in {
    Bag.read(simpleBagDirV0) should matchPattern { case Success(_: Bag) => }
  }

  it should "fail when the directory does not exist" in {
    Bag.read(testDir / 'non / 'existing / 'directory) should matchPattern {
      case Failure(_: NoSuchFileException) =>
    }
  }

  it should "fail when the directory does not represent a valid bag" in {
    val (baseDir, _, _) = createDataDir()
    Bag.read(baseDir) should matchPattern {
      case Failure(e: NoSuchFileException) if e.getMessage == (baseDir / "bagit.txt").toString =>
    }
  }

  "baseDir" should "return the root directory of the bag" in {
    simpleBagV0().baseDir shouldBe simpleBagDirV0
  }

  "data" should "point to the root of the bag/data directory" in {
    val bag = simpleBagV0()
    bag.data.toJava shouldBe (bag.baseDir / "data" toJava)
    bag.data.listRecursively.toList should contain only(
      bag.data / "x",
      bag.data / "y",
      bag.data / "z",
      bag.data / "sub",
      bag.data / "sub" / "u",
      bag.data / "sub" / "v",
      bag.data / "sub" / "w",
    )
  }

  "bagitVersion" should "read the version from bagit.txt" in {
    simpleBagV0().bagitVersion shouldBe new Version(0, 97)
  }

  "withBagitVersion" should "change the version in the bag" in {
    simpleBagV0()
      .withBagitVersion(SupportedVersions.Version_1_0)
      .bagitVersion shouldBe new Version(1, 0)
  }

  "fileEncoding" should "read the file encoding from bagit.txt" in {
    simpleBagV0().fileEncoding shouldBe StandardCharsets.UTF_8
  }

  "withFileEncoding" should "change the file encoding in the bag" in {
    simpleBagV0()
      .withFileEncoding(StandardCharsets.UTF_16LE)
      .fileEncoding shouldBe StandardCharsets.UTF_16LE
  }

  "bagInfo" should "read the data from bag-info.txt" in {
    simpleBagV0().bagInfo should contain only(
      "Payload-Oxum" -> List("72.6"),
      "Bagging-Date" -> List("2016-06-07"),
      "Bag-Size" -> List("0.6 KB"),
      "Created" -> List("2017-01-16T14:35:00.888+01:00"),
    )
  }

  it should "group equivalent keys from bag-info.txt" in {
    multipleKeysBagV0().bagInfo should contain("Bagging-Date" -> List("2016-06-07", "2016-06-08"))
  }

  "addBagInfo" should "add a new key-value pair to the bag-info" in {
    val bag = simpleBagV0()
    val key = "My-Test-Value"
    val value = "hello world"

    val resultBag = bag.addBagInfo(key, value)

    resultBag.bagInfo should contain(key -> List(value))
  }

  it should "add a new value to an already existing key" in {
    val bag = simpleBagV0()
    val key = "Bagging-Date"
    val oldDate = "2016-06-07"

    bag.bagInfo should contain(key -> List(oldDate))

    val newDate = "2018-01-01"
    val resultBag = bag.addBagInfo(key, newDate)

    resultBag.bagInfo should contain(key -> List(oldDate, newDate))
  }

  "removeBagInfo" should "remove a key-value pair from the bag-info" in {
    val bag = simpleBagV0()
    val createdKey = "Created"

    bag.bagInfo should contain key createdKey

    val resultBag = bag.removeBagInfo(createdKey)

    resultBag.bagInfo shouldNot contain key createdKey
  }

  it should "remove all values when the key contains multiple values" in {
    val bag = multipleKeysBagV0()
    val baggingDateKey = "Bagging-Date"

    bag.bagInfo should contain key baggingDateKey
    bag.bagInfo(baggingDateKey) should have length 2

    val resultBag = bag.removeBagInfo(baggingDateKey)

    resultBag.bagInfo shouldNot contain key baggingDateKey
  }

  it should "ignore the removal of a key that is not present in the bagInfo" in {
    val bag = simpleBagV0()
    val nonExistingKey = "Non-Existing-Key"

    bag.bagInfo shouldNot contain key nonExistingKey

    val resultBag = bag.removeBagInfo(nonExistingKey)

    resultBag.bagInfo shouldNot contain key nonExistingKey
  }

  "created" should "return the element in bag-info.txt with key 'Created' if present" in {
    val bag = simpleBagV0()
    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))

    inside(bag.created) {
      case Success(Some(dateTime)) =>
        dateTime.getMillis shouldBe expected.getMillis
    }
  }

  it should "return None if the key 'Created' is not present" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(Bag.CREATED_KEY)

    bag.created should matchPattern { case Success(None) => }
  }

  it should "fail when the key 'Created' contains something that is not a date/time" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(Bag.CREATED_KEY)
    bag.locBag.getMetadata.add(Bag.CREATED_KEY, "not-a-date")

    bag.created should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == "Invalid format: \"not-a-date\"" =>
    }
  }

  "withCreated" should "set the 'Created' key in the bag" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(Bag.CREATED_KEY)

    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))
    val resultBag = bag.withCreated(expected)

    inside(resultBag.created) {
      case Success(Some(dateTime)) => dateTime.getMillis shouldBe expected.getMillis
    }
  }

  it should "discard the former 'Created' key, such that there is always at most one value for this key" in {
    val bag = simpleBagV0()

    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))
    val resultBag = bag.withCreated(expected)

    resultBag.locBag.getMetadata.get(Bag.CREATED_KEY).size() shouldBe 1

    inside(resultBag.created) {
      case Success(Some(dateTime)) => dateTime.getMillis shouldBe expected.getMillis
    }
  }

  "withoutCreated" should "remove the 'Created' key from the bag-info" in {
    val bag = simpleBagV0()

    bag.bagInfo should contain key Bag.CREATED_KEY

    val resultBag = bag.withoutCreated()
    resultBag.bagInfo shouldNot contain key Bag.CREATED_KEY
  }

  it should "not fail when the 'Created' key was not present in the bag-info" in {
    val bag = simpleBagV0()

    val resultBag = bag.withoutCreated()
    resultBag.bagInfo shouldNot contain key Bag.CREATED_KEY

    val resultBag2 = resultBag.withoutCreated()
    resultBag2.bagInfo shouldNot contain key Bag.CREATED_KEY
  }

  "isVersionOf" should "return the urn:uuid of the 'Is-Version-Of' key in the bag-info" in {
    val bag = fetchBagV0()
    val expected = new URI("urn:uuid:00000000-0000-0000-0000-000000000001")

    bag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  it should "return None when no 'Is-Version-Of' key is present in the bag-info" in {
    val bag = simpleBagV0()

    bag.isVersionOf should matchPattern { case Success(None) => }
  }

  it should "fail when the 'Is-Version-Of' key contains something else than a URI" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.add(Bag.IS_VERSION_OF_KEY, "not-a-uri")

    bag.isVersionOf should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == "Invalid format: \"not-a-uri\"" =>
    }
  }

  "withIsVersionOf" should "set the 'Is-Version-Of' key in the bag" in {
    val bag = simpleBagV0()

    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val expected = new URI(s"urn:uuid:$uuid")
    val resultBag = bag.withIsVersionOf(uuid)

    resultBag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  it should "discard the former 'Is-Version-Of' key, such that there is always at most one value for this key" in {
    val bag = fetchBagV0()

    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val expected = new URI(s"urn:uuid:$uuid")
    val resultBag = bag.withIsVersionOf(uuid)

    resultBag.locBag.getMetadata.get(Bag.IS_VERSION_OF_KEY).size() shouldBe 1

    resultBag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  "withoutIsVersionOf" should "remove the 'Is-Version-Of' key from the bag-info" in {
    val bag = fetchBagV0()

    bag.bagInfo should contain key Bag.IS_VERSION_OF_KEY

    val resultBag = bag.withoutIsVersionOf()
    resultBag.bagInfo shouldNot contain key Bag.IS_VERSION_OF_KEY
  }

  it should "not fail when the 'Is-Version-Of' key was not present in the bag-info" in {
    val bag = fetchBagV0()

    bag.bagInfo should contain key Bag.IS_VERSION_OF_KEY

    val resultBag = bag.withoutIsVersionOf()
    resultBag.bagInfo shouldNot contain key Bag.IS_VERSION_OF_KEY

    val resultBag2 = bag.withoutIsVersionOf()
    resultBag2.bagInfo shouldNot contain key Bag.IS_VERSION_OF_KEY
  }

  "fetchFiles" should "list all entries in the fetch.txt files" in {
    val bag = fetchBagV0()
    bag.fetchFiles should contain only(
      FetchItem(lipsum1URL, 12L, bag.data / "sub" / "u"),
      FetchItem(lipsum2URL, 12L, bag.data / "sub" / "v"),
      FetchItem(lipsum3URL, 12L, bag.data / "y-old"),
      FetchItem(lipsum4URL, 12L, bag.data / "x"),
    )
  }

  it should "return an empty list when the bag doesn't have a fetch.txt file" in {
    simpleBagV0().fetchFiles shouldBe empty
  }

  it should "return an empty list when the bag has an empty fetch.txt file" in {
    simpleBagDirV0 / "fetch.txt" touch()

    simpleBagV0().fetchFiles shouldBe empty
  }

  "addFetch" should "add the fetch item to the bag's list of fetch items" in {
    val fetchFileSrc = lipsum1URL
    assumeCanConnect(fetchFileSrc)

    val bag = fetchBagV0()

    inside(bag.addFetchFile(fetchFileSrc, 12345L, _ / "to-be-fetched" / "lipsum2.txt")) {
      case Success(resultBag) =>
        resultBag.fetchFiles should contain(
          FetchItem(fetchFileSrc, 12345L, bag.data / "to-be-fetched" / "lipsum2.txt")
        )
    }
  }

  it should "download the file and calculate all checksums for the file" in {
    val fetchFileSrc = lipsum2URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()

    bag.payloadManifestAlgorithms should contain only(
      ChecksumAlgorithm.SHA1,
      ChecksumAlgorithm.SHA256,
    )
    inside(bag.addFetchFile(fetchFileSrc, 12345L, _ / "to-be-fetched" / "lipsum3.txt")) {
      case Success(resultBag) =>
        val destination = resultBag.data / "to-be-fetched" / "lipsum3.txt"
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(destination -> lipsum2Sha1)
        resultBag.payloadManifests(ChecksumAlgorithm.SHA256) should contain(destination -> lipsum2Sha256)
    }
  }

  it should "remove the file after having calculated the checksums" in {
    val fetchFileSrc = lipsum3URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()

    val beforeListing = bag.listRecursively.toList

    inside(bag.addFetchFile(fetchFileSrc, 12345L, _ / "to-be-fetched" / "lipsum4.txt")) {
      case Success(resultBag) =>
        val afterListing = resultBag.listRecursively.toList

        // While the file was downloaded to the root of the bag, it is removed again once the
        // checksums are calculated.
        afterListing should contain theSameElementsInOrderAs beforeListing
    }
  }

  it should "add the fetch.txt file to all tag manifests when a first fetch.txt file was added" in {
    val fetchFileSrc = lipsum4URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()
    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should not contain key(bag.baseDir / "fetch.txt")
    }

    inside(bag.addFetchFile(fetchFileSrc, 12345L, _ / "to-be-fetched" / "lipsum5.txt")) {
      case Success(resultBag) =>
        forEvery(resultBag.tagManifests) {
          case (_, manifest) =>
            manifest should contain key (resultBag.baseDir / "fetch.txt")
        }
    }
  }

  it should "fail when the destination already exists" in {
    val bag = simpleBagV0()
    val fetchFileSrc = lipsum4URL

    bag.addFetchFile(fetchFileSrc, 12345L, _ / "x") should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "x").toString =>
    }
  }

  it should "fail when the destination is not inside the bag/data directory" in {
    val bag = simpleBagV0()
    val fetchFileSrc = lipsum5URL

    bag.addFetchFile(fetchFileSrc, 12345L, _ / ".." / "lipsum1.txt") should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == s"a fetch file can only point to a location inside the bag/data directory; ${ bag / "lipsum1.txt" } is outside the data directory" =>
    }
  }

  it should "fail when the file cannot be downloaded from the provided url" in {
    val bag = simpleBagV0()

    inside(bag.addFetchFile(new URL("http://x"), 12345L, _ / "to-be-fetched" / "failing-url.txt")) {
      // it's either one of these exceptions that is thrown
      case Failure(e: SocketTimeoutException) =>
        e should have message "connect timed out"
      case Failure(e: UnknownHostException) =>
        e should have message "x"
    }
  }

  "removeFetchByFile" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val relativePath: RelativePath = _ / "x"
    val absolutePath = relativePath(bag.data)

    bag.fetchFiles.map(_.file) should contain(absolutePath)

    inside(bag.removeFetchByFile(relativePath)) {
      case Success(resultBag) =>
        resultBag.fetchFiles.map(_.file) should not contain absolutePath
    }
  }

  it should "remove the fetch item from all payload manifests" in {
    val bag = fetchBagV0()
    val relativePath: RelativePath = _ / "x"
    val absolutePath = relativePath(bag.data)

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key absolutePath
    }

    inside(bag.removeFetchByFile(relativePath)) {
      case Success(resultBag) =>
        forEvery(resultBag.payloadManifests) {
          case (_, manifest) =>
            manifest shouldNot contain key absolutePath
        }
    }
  }

  it should "remove fetch.txt from all tag manifests when the last fetch file was removed" in {
    val bag = fetchBagV0()

    val relativePath1: RelativePath = _ / "x"
    val relativePath2: RelativePath = _ / "y-old"
    val relativePath3: RelativePath = _ / "sub" / "u"
    val relativePath4: RelativePath = _ / "sub" / "v"

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bag.baseDir / "fetch.txt"
    }

    val result = bag.removeFetchByFile(relativePath1)
      .flatMap(_.removeFetchByFile(relativePath2))
      .flatMap(_.removeFetchByFile(relativePath3))
      .flatMap(_.removeFetchByFile(relativePath4))

    inside(result) {
      case Success(resultBag) =>
        forEvery(resultBag.tagManifests) {
          case (_, manifest) =>
            manifest shouldNot contain key bag.baseDir / "fetch.txt"
        }
    }
  }

  it should "fail when the fetch item is not a part of the bag" in {
    val bag = fetchBagV0()
    val relativePath: RelativePath = _ / "not-existing-fetch-file"
    val absolutePath = relativePath(bag.data)

    bag.fetchFiles.map(_.file) should not contain absolutePath

    inside(bag.removeFetchByFile(relativePath)) {
      case Failure(e: NoSuchFileException) =>
        e should have message absolutePath.toString
        bag.fetchFiles.map(_.file) should not contain absolutePath
    }
  }

  "removeFetchByURL" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val url = lipsum1URL

    bag.fetchFiles.map(_.url) should contain(url)

    inside(bag.removeFetchByURL(url)) {
      case Success(resultBag) =>
        resultBag.fetchFiles.map(_.url) should not contain url
    }
  }

  it should "remove the fetch item from all payload manifests" in {
    val bag = fetchBagV0()
    val url = lipsum1URL

    inside(bag.fetchFiles.find(_.url == url).map(_.file)) {
      case Some(absolutePath) =>
        forEvery(bag.payloadManifests) {
          case (_, manifest) =>
            manifest should contain key absolutePath
        }

        inside(bag.removeFetchByURL(url)) {
          case Success(resultBag) =>
            forEvery(resultBag.payloadManifests) {
              case (_, manifest) =>
                manifest shouldNot contain key absolutePath
            }
        }
    }
  }

  it should "remove fetch.txt from all tag manifests when the last fetch file was removed" in {
    val bag = fetchBagV0()

    val url1 = lipsum1URL
    val url2 = lipsum2URL
    val url3 = lipsum3URL
    val url4 = lipsum4URL

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bag.baseDir / "fetch.txt"
    }

    val result = bag.removeFetchByURL(url1)
      .flatMap(_.removeFetchByURL(url2))
      .flatMap(_.removeFetchByURL(url3))
      .flatMap(_.removeFetchByURL(url4))

    inside(result) {
      case Success(resultBag) =>
        forEvery(resultBag.tagManifests) {
          case (_, manifest) =>
            manifest shouldNot contain key bag.baseDir / "fetch.txt"
        }
    }
  }

  it should "fail when the fetch item is not a part of the bag" in {
    val bag = fetchBagV0()
    val url = new URL("http://not.existing/fetch/file")

    bag.fetchFiles.map(_.url) should not contain url

    inside(bag.removeFetchByURL(url)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"no such URL: $url"
        bag.fetchFiles.map(_.url) should not contain url
    }
  }

  "removeFetch" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val fetchItem = bag.fetchFiles.head

    val resultBag = bag.removeFetch(fetchItem)
    resultBag.fetchFiles should not contain fetchItem
  }

  it should "remove the fetch item from all payload manifests" in {
    val bag = fetchBagV0()
    val fetchItem = bag.fetchFiles.head

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key fetchItem.file
    }

    val resultBag = bag.removeFetch(fetchItem)

    forEvery(resultBag.payloadManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key fetchItem.file
    }
  }

  it should "remove fetch.txt from all tag manifests when the last fetch file was removed" in {
    val bag = fetchBagV0()

    val fetch1 :: fetch2 :: fetch3 :: fetch4 :: Nil = bag.fetchFiles.toList

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bag.baseDir / "fetch.txt"
    }

    val resultBag = bag.removeFetch(fetch1)
      .removeFetch(fetch2)
      .removeFetch(fetch3)
      .removeFetch(fetch4)

    forEvery(resultBag.tagManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key bag.baseDir / "fetch.txt"
    }
  }

  it should "not fail when the fetch item is not a part of the bag" in {
    val bag = fetchBagV0()
    val fetchItem = FetchItem(new URL("http://not.existing/fetch/file"), 12L, bag.data / "not-existing-fetch-file")

    bag.fetchFiles should not contain fetchItem
    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key fetchItem.file
    }

    val resultBag = bag.removeFetch(fetchItem)

    resultBag.fetchFiles should not contain fetchItem
    forEvery(resultBag.payloadManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key fetchItem.file
    }
  }

  "payloadManifestAlgorithms" should "list all payload manifest algorithms that are being used in this bag" in {
    val bag = multipleManifestsBagV0()

    bag.payloadManifestAlgorithms should contain only(
      ChecksumAlgorithm.SHA1,
      ChecksumAlgorithm.SHA256,
    )
  }

  "addPayloadManifestAlgorithm" should "add a checksum algorithm to a bag and calculate the " +
    "checksum for all payload files" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.MD5

    inside(bag.addPayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should contain only(
          algorithm,
          ChecksumAlgorithm.SHA1,
        )

        resultBag.payloadManifests should containInManifestOnly(algorithm)(
          resultBag.data / "x",
          resultBag.data / "y",
          resultBag.data / "z",
          resultBag.data / "sub" / "u",
          resultBag.data / "sub" / "v",
          resultBag.data / "sub" / "w"
        )
    }
  }

  it should "by default not update the checksums when the algorithm is already used by the bag" in {
    val bag = simpleBagV0()
    val algorithm = bag.payloadManifestAlgorithms.head
    val specialFile = bag.data / "x"
    val specialChecksum = "messed up checksum"

    bag.locBag.getPayLoadManifests.asScala.head
      .getFileToChecksumMap
      .put(specialFile, "messed up checksum")

    inside(bag.addPayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should contain only ChecksumAlgorithm.SHA1

        resultBag.payloadManifests(algorithm)(specialFile) shouldBe specialChecksum
    }
  }

  it should "when indicated, update the checksums when the algorithm is already used by the bag" in {
    val bag = simpleBagV0()
    val algorithm = bag.payloadManifestAlgorithms.head
    val specialFile = bag.data / "x"
    val specialChecksum = "messed up checksum"

    bag.locBag.getPayLoadManifests.asScala.head
      .getFileToChecksumMap
      .put(specialFile, "messed up checksum")

    inside(bag.addPayloadManifestAlgorithm(algorithm, updateManifest = true)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should contain only ChecksumAlgorithm.SHA1

        resultBag.payloadManifests(algorithm)(specialFile) should not be specialChecksum

        resultBag.payloadManifests should containInManifestOnly(algorithm)(
          resultBag.data / "x",
          resultBag.data / "y",
          resultBag.data / "z",
          resultBag.data / "sub" / "u",
          resultBag.data / "sub" / "v",
          resultBag.data / "sub" / "w"
        )
    }
  }

  it should "not calculate any checksums when the bag contains no payload" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.MD5
    bag.data.delete().createDirectories()
    bag.data.isEmpty shouldBe true

    inside(bag.addPayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should contain only(
          algorithm,
          ChecksumAlgorithm.SHA1,
        )

        resultBag.payloadManifests(algorithm) shouldBe empty
    }
  }

  it should "add the new manifest file to all tagmanifests present in the bag" in {
    val bag = multipleManifestsBagV0()
    val algorithm = ChecksumAlgorithm.MD5

    val tagManifestsBefore = bag.tagManifests
    forEvery(tagManifestsBefore.keySet)(algo => {
      tagManifestsBefore(algo) shouldNot contain key bag / s"manifest-${ algorithm.getBagitName }.txt"
    })

    inside(bag.addPayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        val tagManifestsAfter = resultBag.tagManifests
        forEvery(tagManifestsAfter.keySet)(algo => {
          tagManifestsAfter(algo) should contain key bag / s"manifest-${ algorithm.getBagitName }.txt"
        })
    }
  }

  it should "calculate and add the checksums of fetch files to the newly added manifest" in {
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL)
    val bag = fetchBagV0()
    val algorithm = ChecksumAlgorithm.MD5

    inside(bag.addPayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifests(algorithm) should contain allOf(
          resultBag.data / "sub" / "u" -> lipsum1Md5,
          resultBag.data / "sub" / "v" -> lipsum2Md5,
          resultBag.data / "y-old" -> lipsum3Md5,
          resultBag.data / "x" -> lipsum4Md5,
        )
    }
  }

  "removePayloadManifestAlgorithm" should "remove a checksum algorithm from a bag, as well as " +
    "all it's checksums" in {
    val bag = multipleManifestsBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    bag.payloadManifestAlgorithms should contain only(
      algorithm,
      ChecksumAlgorithm.SHA256
    )

    inside(bag.removePayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should not contain algorithm
        resultBag.payloadManifestAlgorithms should contain only ChecksumAlgorithm.SHA256
    }
  }

  it should "succeed when it removes the last checksum algorithm from the bag (should fail on save instead)" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    bag.payloadManifestAlgorithms should contain only algorithm

    inside(bag.removePayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.payloadManifestAlgorithms should not contain algorithm
        resultBag.payloadManifestAlgorithms shouldBe empty
    }
  }

  it should "remove the manifest file from all tagmanifests present in the bag" in {
    val bag = multipleManifestsBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    val tagManifestsBefore = bag.tagManifests
    forEvery(tagManifestsBefore.keySet)(algo => {
      tagManifestsBefore(algo) should contain key bag / s"manifest-${ algorithm.getBagitName }.txt"
    })

    inside(bag.removePayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        val tagManifestsAfter = resultBag.tagManifests
        forEvery(tagManifestsAfter.keySet)(algo => {
          tagManifestsAfter(algo) shouldNot contain key bag / s"manifest-${ algorithm.getBagitName }.txt"
        })
    }
  }

  it should "fail when the checksum algorithm is not present in the bag" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.SHA256

    bag.payloadManifestAlgorithms should not contain algorithm

    inside(bag.removePayloadManifestAlgorithm(algorithm)) {
      case Failure(e: NoSuchElementException) =>
        e should have message s"No manifest found for checksum $algorithm"
    }
  }

  "tagManifestAlgorithms" should "list all tag manifest algorithms that are being used in this bag" in {
    val bag = multipleManifestsBagV0()

    bag.tagManifestAlgorithms should contain only(
      ChecksumAlgorithm.SHA1,
      ChecksumAlgorithm.SHA256,
    )
  }

  "addTagManifestAlgorithm" should "add a checksum algorithm to a bag and calculate the " +
    "checksum for all tag files" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.MD5

    inside(bag.addTagManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.tagManifestAlgorithms should contain only(
          algorithm,
          ChecksumAlgorithm.SHA1,
        )

        resultBag.tagManifests should containInManifestOnly(algorithm)(
          bag / "bagit.txt",
          bag / "bag-info.txt",
          bag / "manifest-sha1.txt",
          bag / "metadata/dataset.xml",
          bag / "metadata/files.xml"
        )
    }
  }

  it should "by default not update the checksums when the algorithm is already used by the bag" in {
    val bag = simpleBagV0()
    val algorithm = bag.tagManifestAlgorithms.head
    val specialFile = bag.data / "x"
    val specialChecksum = "messed up checksum"

    bag.locBag.getTagManifests.asScala.head
      .getFileToChecksumMap
      .put(specialFile, "messed up checksum")

    inside(bag.addTagManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.tagManifestAlgorithms should contain only ChecksumAlgorithm.SHA1

        resultBag.tagManifests(algorithm)(specialFile) shouldBe specialChecksum
    }
  }

  it should "when indicated, update the checksums when the algorithm is already used by the bag" in {
    val bag = simpleBagV0()
    val algorithm = bag.tagManifestAlgorithms.head
    val specialFile = bag / "bagit.txt"
    val specialChecksum = "messed up checksum"

    bag.locBag.getTagManifests.asScala.head
      .getFileToChecksumMap
      .put(specialFile, "messed up checksum")

    inside(bag.addTagManifestAlgorithm(algorithm, updateManifest = true)) {
      case Success(resultBag) =>
        resultBag.tagManifestAlgorithms should contain only ChecksumAlgorithm.SHA1

        resultBag.tagManifests(algorithm)(specialFile) should not be specialChecksum

        resultBag.tagManifests should containInManifestOnly(algorithm)(
          bag / "bagit.txt",
          bag / "bag-info.txt",
          bag / "manifest-sha1.txt",
          bag / "metadata/dataset.xml",
          bag / "metadata/files.xml"
        )
    }
  }

  "removeTagManifestAlgorithm" should "remove a checksum algorithm from a bag, as well as all " +
    "it's checksums" in {
    val bag = multipleManifestsBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    bag.tagManifestAlgorithms should contain only(
      algorithm,
      ChecksumAlgorithm.SHA256
    )

    inside(bag.removeTagManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.tagManifestAlgorithms should not contain algorithm
        resultBag.tagManifestAlgorithms should contain only ChecksumAlgorithm.SHA256
    }
  }

  it should "succeed when it removes the last checksum algorithm from the bag (should fail on save instead)" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    bag.tagManifestAlgorithms should contain only algorithm

    inside(bag.removeTagManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        resultBag.tagManifestAlgorithms should not contain algorithm
        resultBag.tagManifestAlgorithms shouldBe empty
    }
  }

  it should "fail when the checksum algorithm is not present in the bag" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.SHA256

    bag.tagManifestAlgorithms should not contain algorithm

    inside(bag.removeTagManifestAlgorithm(algorithm)) {
      case Failure(e: NoSuchElementException) =>
        e should have message s"No manifest found for checksum $algorithm"
    }
  }

  "payloadManifests" should "list all entries in the manifest-<alg>.txt files" in {
    val bag = multipleManifestsBagV0()
    val algorithms = List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256)

    bag.payloadManifests.keySet should contain theSameElementsAs algorithms

    forEvery(algorithms)(algorithm => {
      bag.payloadManifests should containInManifestOnly(algorithm)(
        bag.data / "sub" / "u",
        bag.data / "sub" / "v",
        bag.data / "sub" / "w",
        bag.data / "x",
        bag.data / "z",
        bag.data / "y"
      )
    })
  }

  def addPayloadFile(addPayloadFile: Bag => File => RelativePath => Try[Bag]): Unit = {
    it should "copy the new file into the bag" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag.data)

      dest.toJava shouldNot exist

      addPayloadFile(bag)(file)(relativeDest) shouldBe a[Success[_]]

      dest.toJava should exist
      dest.contentAsString shouldBe file.contentAsString
      dest.sha1 shouldBe file.sha1
    }

    it should "add the checksum for all algorithms in the bag to the payload manifest" in {
      val bag = multipleManifestsBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag.data)

      bag.payloadManifestAlgorithms should contain only(
        ChecksumAlgorithm.SHA1,
        ChecksumAlgorithm.SHA256,
      )

      inside(addPayloadFile(bag)(file)(relativeDest)) {
        case Success(resultBag) =>
          resultBag.payloadManifestAlgorithms should contain only(
            ChecksumAlgorithm.SHA1,
            ChecksumAlgorithm.SHA256,
          )

          resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest
          resultBag.payloadManifests(ChecksumAlgorithm.SHA1)(dest) shouldBe dest.sha1.toLowerCase

          resultBag.payloadManifests(ChecksumAlgorithm.SHA256) should contain key dest
          resultBag.payloadManifests(ChecksumAlgorithm.SHA256)(dest) shouldBe dest.sha256.toLowerCase
      }
    }

    it should "fail when the destination is outside the bag/data directory" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / ".." / ".." / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag.data)

      dest.toJava shouldNot exist

      inside(addPayloadFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"pathInData '$dest' is supposed to point to a file that is a child of the bag/data directory"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination already exists" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / ".." / ".." / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag.data) createIfNotExists (createParents = true) writeText lipsum(1)

      dest.toJava should exist

      inside(addPayloadFile(bag)(file)(relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message dest.toString
      }

      dest.toJava should exist
      dest.contentAsString shouldBe lipsum(1)
    }

    it should "fail when the destination is already mentioned in the fetch file" in {
      val bag = fetchBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "x"
      val dest = relativeDest(bag.data)

      dest.toJava shouldNot exist
      bag.fetchFiles.map(_.file) contains dest
      bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest
      val sha1Before = bag.payloadManifests(ChecksumAlgorithm.SHA1)(dest)
      file.sha1 should not be sha1Before

      inside(addPayloadFile(bag)(file)(relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message s"$dest: file already present in bag as a fetch file"

          dest.toJava shouldNot exist
          bag.fetchFiles.map(_.file) contains dest
          bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest

          val sha1After = bag.payloadManifests(ChecksumAlgorithm.SHA1)(dest)
          sha1Before shouldBe sha1After
      }
    }
  }

  "addPayloadFile with stream" should behave like addPayloadFile(
    bag => file => relativeDest => file.inputStream()(bag.addPayloadFile(_)(relativeDest))
  )

  it should "fail when the given file is a directory" in {
    val bag = simpleBagV0()
    val newDir = testDir / "newDir" createDirectories()
    newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    newDir / "sub" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    newDir / "sub" / "subsub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val relativeDest: RelativePath = _ / "path" / "to" / "newDir"

    inside(newDir.inputStream()(bag.addPayloadFile(_)(relativeDest))) {
      case Failure(e: IOException) =>
        e should have message "Is a directory"
    }
  }

  "addPayloadFile with file" should behave like addPayloadFile(_.addPayloadFile)

  it should "recursively add the files and folders in the directory to the bag" in {
    val bag = multipleManifestsBagV0()
    val newDir = testDir / "newDir" createDirectories()
    val file1 = newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    val file2 = newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    val file3 = newDir / "sub1" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    val file4 = newDir / "sub1" / "sub1sub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val file5 = newDir / "sub2" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    val relativeDest: RelativePath = _ / "path" / "to" / "newDir"
    val dest = relativeDest(bag.data)

    inside(bag.addPayloadFile(newDir)(relativeDest)) {
      case Success(resultBag) =>
        val files = Set(file1, file2, file3, file4, file5)
          .map(file => dest / newDir.relativize(file).toString)

        forEvery(files)(file => {
          file.toJava should exist

          forEvery(List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256))(algo => {
            resultBag.payloadManifests(algo) should contain key file
            resultBag.payloadManifests(algo)(file) shouldBe file.checksum(algo).toLowerCase
          })
        })
    }
  }

  "removePayloadFile" should "remove a payload file from the bag" in {
    val bag = simpleBagV0()
    val file = bag.data / "sub" / "u"

    file.toJava should exist

    bag.removePayloadFile(_ / "sub" / "u") shouldBe a[Success[_]]
    file.toJava shouldNot exist
    file.parent.toJava should exist
  }

  it should "remove the removed payload file from all manifests" in {
    val bag = multipleManifestsBagV0()
    val file = bag.data / "x"

    bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key file
    bag.payloadManifests(ChecksumAlgorithm.SHA256) should contain key file

    inside(bag.removePayloadFile(_ / "x")) {
      case Success(resultBag) =>
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key file
        resultBag.payloadManifests(ChecksumAlgorithm.SHA256) shouldNot contain key file
    }
  }

  it should "clean up empty directories after the last payload file in a directory is removed" in {
    val bag = simpleBagV0()
    val file = testDir / "a.txt" createIfNotExists (createParents = true) writeText "content of file a"
    val destination = bag.data / "path" / "to" / "file" / "a.txt"

    bag.addPayloadFile(file)(_ / "path" / "to" / "file" / "a.txt") shouldBe a[Success[_]]

    destination.toJava should exist
    bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key destination

    inside(bag.removePayloadFile(_ / "path" / "to" / "file" / "a.txt")) {
      case Success(resultBag) =>
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key destination

        destination.toJava shouldNot exist
        destination.parent.toJava shouldNot exist
        destination.parent.parent.toJava shouldNot exist
        destination.parent.parent.parent.toJava shouldNot exist
        destination.parent.parent.parent.parent.toJava should exist
        destination.parent.parent.parent.parent shouldBe resultBag.data
    }
  }

  it should "keep an empty bag/data directory if all files are removed from the bag" in {
    val bag = simpleBagV0()

    val result = bag.removePayloadFile(_ / "x")
      .flatMap(_.removePayloadFile(_ / "y"))
      .flatMap(_.removePayloadFile(_ / "z"))
      .flatMap(_.removePayloadFile(_ / "sub" / "u"))
      .flatMap(_.removePayloadFile(_ / "sub" / "v"))
      .flatMap(_.removePayloadFile(_ / "sub" / "w"))

    inside(result) {
      case Success(resultBag) =>
        resultBag.data.toJava should exist
        resultBag.data.children should have size 0
    }
  }

  it should "remove empty payload manifests from the bag" in {
    val bag = multipleManifestsBagV0()

    val files = List(
      bag.data / "x",
      bag.data / "y",
      bag.data / "z",
      bag.data / "sub" / "u",
      bag.data / "sub" / "v",
      bag.data / "sub" / "w"
    )

    forEvery(List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256))(algo => {
      bag.payloadManifests should contain key algo

      forEvery(files)(file => {
        bag.payloadManifests(algo) should contain key file
      })
    })

    val result = bag.removePayloadFile(_ / "x")
      .flatMap(_.removePayloadFile(_ / "y"))
      .flatMap(_.removePayloadFile(_ / "z"))
      .flatMap(_.removePayloadFile(_ / "sub" / "u"))
      .flatMap(_.removePayloadFile(_ / "sub" / "v"))
      .flatMap(_.removePayloadFile(_ / "sub" / "w"))

    inside(result) {
      case Success(resultBag) =>
        resultBag.payloadManifests shouldBe empty
    }
  }

  it should "fail when the file does not exist" in {
    val bag = simpleBagV0()
    val file = bag.data / "doesnotexist.txt"

    file.toJava shouldNot exist

    inside(bag.removePayloadFile(_ / "doesnotexist.txt")) {
      case Failure(e: NoSuchFileException) =>
        e should have message file.toString
    }
  }

  it should "fail when the file is not inside the bag/data directory" in {
    val bag = simpleBagV0()
    val file = bag / "bagit.txt"

    file.toJava should exist
    file.isChildOf(bag.data) shouldBe false

    inside(bag.removePayloadFile(_ / ".." / "bagit.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"pathInData '$file' is supposed to point to a file that is a " +
          s"child of the bag/data directory"
    }
  }

  it should "fail when the file to be removed is a directory" in {
    val bag = simpleBagV0()
    val file = bag.data / "sub"

    file.toJava should exist
    file.isDirectory shouldBe true
    val checksumsBeforeCall = bag.payloadManifests

    inside(bag.removePayloadFile(_ / "sub")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove directory '$file'; you can only remove files"

        file.toJava should exist
        file.isDirectory shouldBe true

        // payload manifests didn't change
        bag.payloadManifests shouldBe checksumsBeforeCall
    }
  }

  "tagManifests" should "list all entries in the tagmanifest-<alg>.txt files" in {
    val bag = multipleManifestsBagV0()
    val algorithms = List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256)

    bag.tagManifests.keySet should contain theSameElementsAs algorithms

    forEvery(algorithms)(algorithm => {
      bag.tagManifests should containInManifestOnly(algorithm)(
        bag / "bagit.txt",
        bag / "bag-info.txt",
        bag / "manifest-sha1.txt",
        bag / "manifest-sha256.txt",
        bag / "metadata/dataset.xml",
        bag / "metadata/files.xml"
      )
    })
  }

  def addTagFile(addTagFile: Bag => File => RelativePath => Try[Bag]): Unit = {
    it should "copy the new file into the bag" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "metadata" / "temp" / "file-copy.txt"
      val dest = relativeDest(bag)

      dest.toJava shouldNot exist

      addTagFile(bag)(file)(relativeDest) shouldBe a[Success[_]]

      dest.toJava should exist
      dest.contentAsString shouldBe file.contentAsString
      dest.sha1 shouldBe file.sha1
    }

    it should "add the checksum for all algorithms in the bag to the tag manifests" in {
      val bag = multipleManifestsBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "metadata" / "temp" / "file-copy.txt"
      val dest = relativeDest(bag)

      bag.tagManifestAlgorithms should contain only(
        ChecksumAlgorithm.SHA1,
        ChecksumAlgorithm.SHA256,
      )

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Success(resultBag) =>
          resultBag.tagManifestAlgorithms should contain only(
            ChecksumAlgorithm.SHA1,
            ChecksumAlgorithm.SHA256,
          )

          resultBag.tagManifests(ChecksumAlgorithm.SHA1) should contain key dest
          resultBag.tagManifests(ChecksumAlgorithm.SHA1)(dest) shouldBe dest.sha1.toLowerCase

          resultBag.tagManifests(ChecksumAlgorithm.SHA256) should contain key dest
          resultBag.tagManifests(ChecksumAlgorithm.SHA256)(dest) shouldBe dest.sha256.toLowerCase
      }
    }

    it should "fail when the destination is inside the bag/data directory" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "data" / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag)

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"cannot add a tag file like '$dest' to the bag/data directory"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination is outside the bag directory" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / ".." / "path" / "to" / "file-copy.txt"
      val dest = relativeDest(bag)

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"cannot add a tag file like '$dest' to a place outside the bag directory"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination already exists" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "metadata" / "dataset.xml"
      val dest = relativeDest(bag)

      dest.toJava should exist
      val oldContent = dest.contentAsString

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message dest.toString
      }

      dest.toJava should exist
      dest.contentAsString shouldBe oldContent
    }

    it should "fail when the destination points to bag/bag-info.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "bag-info.txt"
      val dest = relativeDest(bag)

      dest.delete()

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'bag-info.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination points to bag/bagit.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "bagit.txt"
      val dest = relativeDest(bag)

      dest.delete()

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'bagit.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination points to bag/fetch.txt, which was removed first" in {
      val bag = fetchBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "fetch.txt"
      val dest = relativeDest(bag)

      dest.delete()

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'fetch.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination points to bag/manifest-XXX.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "manifest-sha1.txt"
      val dest = relativeDest(bag)

      dest.delete()

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "manifest files are controlled by the library itself; you cannot add a file to this location"
      }

      dest.toJava shouldNot exist
    }

    it should "fail when the destination points to bag/tagmanifest-XXX.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest: RelativePath = _ / "tagmanifest-sha1.txt"
      val dest = relativeDest(bag)

      dest.delete()

      dest.toJava shouldNot exist

      inside(addTagFile(bag)(file)(relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tagmanifest files are controlled by the library itself; you cannot add a file to this location"
      }

      dest.toJava shouldNot exist
    }
  }

  "addTagFile with stream" should behave like addTagFile(
    bag => file => relativeDest => file.inputStream()(bag.addTagFile(_)(relativeDest))
  )

  it should "fail when the given file is a directory" in {
    val bag = simpleBagV0()
    val newDir = testDir / "newDir" createDirectories()
    newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    newDir / "sub" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    newDir / "sub" / "subsub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val relativeDest: RelativePath = _ / "path" / "to" / "newDir"

    inside(newDir.inputStream()(bag.addTagFile(_)(relativeDest))) {
      case Failure(e: IOException) =>
        e should have message "Is a directory"
    }
  }

  "addTagFile with file" should behave like addTagFile(_.addTagFile)

  it should "recursively add the files and folders in the directory to the bag" in {
    val bag = multipleManifestsBagV0()
    val newDir = testDir / "newDir" createDirectories()
    val file1 = newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    val file2 = newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    val file3 = newDir / "sub1" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    val file4 = newDir / "sub1" / "sub1sub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val file5 = newDir / "sub2" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    val relativeDest: RelativePath = _ / "path" / "to" / "newDir"
    val dest = relativeDest(bag)

    inside(bag.addTagFile(newDir)(relativeDest)) {
      case Success(resultBag) =>
        val files = Set(file1, file2, file3, file4, file5)
          .map(file => dest / newDir.relativize(file).toString)

        forEvery(files)(file => {
          file.toJava should exist

          forEvery(List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256))(algo => {
            resultBag.tagManifests(algo) should contain key file
            resultBag.tagManifests(algo)(file) shouldBe file.checksum(algo).toLowerCase
          })
        })
    }
  }

  "removeTagFile" should "remove a tag file from the bag" in {
    val bag = simpleBagV0()
    val file = bag / "metadata" / "dataset.xml"

    file.toJava should exist

    bag.removeTagFile(_ / "metadata" / "dataset.xml") shouldBe a[Success[_]]
    file.toJava shouldNot exist
    file.parent shouldBe bag / "metadata"
    file.parent.toJava should exist
  }

  it should "remove the removed tag file from all manifests" in {
    val bag = multipleManifestsBagV0()
    val file = bag / "metadata" / "dataset.xml"

    bag.tagManifests(ChecksumAlgorithm.SHA1) should contain key file
    bag.tagManifests(ChecksumAlgorithm.SHA256) should contain key file

    inside(bag.removeTagFile(_ / "metadata" / "dataset.xml")) {
      case Success(resultBag) =>
        resultBag.tagManifests(ChecksumAlgorithm.SHA1) shouldNot contain key file
        resultBag.tagManifests(ChecksumAlgorithm.SHA256) shouldNot contain key file
    }
  }

  it should "clean up empty directories after the last tag file in a directory is removed" in {
    val bag = simpleBagV0()
    val file = testDir / "a.txt" createIfNotExists (createParents = true) writeText "content of file a"
    val destination = bag / "path" / "to" / "file" / "a.txt"

    bag.addTagFile(file)(_ / "path" / "to" / "file" / "a.txt") shouldBe a[Success[_]]

    destination.toJava should exist
    bag.tagManifests(ChecksumAlgorithm.SHA1) should contain key destination

    inside(bag.removeTagFile(_ / "path" / "to" / "file" / "a.txt")) {
      case Success(resultBag) =>
        resultBag.tagManifests(ChecksumAlgorithm.SHA1) shouldNot contain key destination

        destination.toJava shouldNot exist
        destination.parent.toJava shouldNot exist
        destination.parent.parent.toJava shouldNot exist
        destination.parent.parent.parent.toJava shouldNot exist
        destination.parent.parent.parent.parent shouldBe resultBag.baseDir
        destination.parent.parent.parent.parent.toJava should exist
    }
  }

  it should "fail when the file does not exist" in {
    val bag = simpleBagV0()
    val file = bag / "doesnotexist.txt"

    file.toJava shouldNot exist

    inside(bag.removeTagFile(_ / "doesnotexist.txt")) {
      case Failure(e: NoSuchFileException) => e should have message file.toString
    }
  }

  it should "fail when the file is inside the bag/data directory" in {
    val bag = simpleBagV0()
    val file = bag.data / "x"

    file.toJava should exist
    file.isChildOf(bag.data) shouldBe true

    inside(bag.removeTagFile(_ / "data" / "x")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove '$file' since it is a child of the bag/data directory"
    }
  }

  it should "fail when the file is outside the bag directory" in {
    val bag = simpleBagV0()
    val file = bag / ".." / "temp.txt" createIfNotExists() writeText "content of temp.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe false

    inside(bag.removeTagFile(_ / ".." / "temp.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove '$file' since it is not a child of the bag directory"
    }
  }

  it should "fail when the file to remove is the bag directory itself" in {
    val bag = simpleBagV0()

    bag.toJava should exist
    //    bag.isChildOf(bag) shouldBe false // TODO why does this actually return true??? - https://github.com/pathikrit/better-files/issues/247

    inside(bag.removeTagFile(identity)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "cannot remove the whole bag"
    }
  }

  it should "fail when the file is the bag/bag-info.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "bag-info.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "bag-info.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is the bag/bagit.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "bagit.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "bagit.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is the bag/fetch.txt file" in {
    val bag = fetchBagV0()
    val file = bag / "fetch.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "fetch.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is a bag/manifest-XXX.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "manifest-sha1.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "manifest-sha1.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove manifest file '$file'"
    }
  }

  it should "fail when the file is a bag/tagmanifest-XXX.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "tagmanifest-sha1.txt"

    file.toJava should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "tagmanifest-sha1.txt")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove tagmanifest file '$file'"
    }
  }

  it should "fail when the file is a directory" in {
    val bag = simpleBagV0()
    val dir = bag / "metadata"

    dir.toJava should exist
    dir.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(_ / "metadata")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove directory '$dir'; you can only remove files"
    }
  }

  "save" should "save bagit.txt" in {
    val bag = simpleBagV0()
    val bagitTxt = bag / "bagit.txt"

    // initial assumptions
    bag.bagitVersion shouldBe new Version(0, 97)
    bag.fileEncoding shouldBe StandardCharsets.UTF_8
    bagitTxt.lines should contain only(
      "BagIt-Version: 0.97",
      "Tag-File-Character-Encoding: UTF-8",
    )

    // changes + save
    bag.withBagitVersion(0, 96)
      .withFileEncoding(StandardCharsets.ISO_8859_1)
      .save() shouldBe a[Success[_]]

    // expected results
    bagitTxt.lines should contain only(
      "BagIt-Version: 0.96",
      "Tag-File-Character-Encoding: ISO-8859-1",
    )

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.TAG_FILES_ENCODING,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM
    )
  }

  it should "save bag-info.txt with updated payload-oxum and bagging-date" in {
    val bag = simpleBagV0()
    val bagInfoTxt = bag / "bag-info.txt"

    // initial assumptions
    bag.bagInfo should contain only(
      "Payload-Oxum" -> Seq("72.6"),
      "Bagging-Date" -> Seq("2016-06-07"),
      "Bag-Size" -> Seq("0.6 KB"),
      "Created" -> Seq("2017-01-16T14:35:00.888+01:00"),
    )
    bagInfoTxt.lines should contain only(
      "Payload-Oxum: 72.6",
      "Bagging-Date: 2016-06-07",
      "Bag-Size: 0.6 KB",
      "Created: 2017-01-16T14:35:00.888+01:00",
    )

    // changes + save
    val newFile = testDir / "xxx.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    val uuid = s"urn:uuid:${ UUID.randomUUID() }"
    bag.addPayloadFile(newFile)(_ / "abc.txt")
      .map(_.addBagInfo("Is-Version-Of", uuid))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    val today = DateTime.now().toString(ISODateTimeFormat.yearMonthDay())
    bag.bagInfo should contain only(
      "Payload-Oxum" -> Seq("2592.7"),
      "Bagging-Date" -> Seq(today),
      "Bag-Size" -> Seq("0.6 KB"),
      "Created" -> Seq("2017-01-16T14:35:00.888+01:00"),
      "Is-Version-Of" -> Seq(uuid),
    )
    bag.baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("2592.7"),
      "Bagging-Date" -> Seq(today),
      "Bag-Size" -> Seq("0.6 KB"),
      "Created" -> Seq("2017-01-16T14:35:00.888+01:00"),
      "Is-Version-Of" -> Seq(uuid)
    )

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "not save or create a fetch.txt when no fetch files were ever present" in {
    val bag = simpleBagV0()
    val fetchTxt = bag / "fetch.txt"

    // initial assumptions
    bag.fetchFiles shouldBe empty
    fetchTxt.toJava shouldNot exist

    // (no) changes + save
    bag.save() shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles shouldBe empty
    fetchTxt.toJava shouldNot exist

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "create a fetch.txt file when fetch files were introduced to the bag for the first time" in {
    assumeCanConnect(lipsum1URL, lipsum2URL)

    val bag = multipleManifestsBagV0()

    val fetchTxt = bag / "fetch.txt"
    val fetchItem1 = FetchItem(lipsum1URL, 12L, bag.data / "some-file1.txt")
    val fetchItem2 = FetchItem(lipsum2URL, 13L, bag.data / "some-file2.txt")

    // initial assumptions
    bag.fetchFiles shouldBe empty
    fetchTxt.toJava shouldNot exist

    // changes + save
    bag.addFetchFile(lipsum1URL, 12L, _ / "some-file1.txt")
      .flatMap(_.addFetchFile(lipsum2URL, 13L, _ / "some-file2.txt"))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles should contain only(
      fetchItem1,
      fetchItem2,
    )
    fetchTxt.toJava should exist
    bag.baseDir should containInFetchOnly(
      fetchItem1,
      fetchItem2
    )

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "save fetch.txt when there were already fetch files in the bag" ignore { // TODO https://github.com/LibraryOfCongress/bagit-java/issues/117
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL, lipsum5URL)

    val bag = fetchBagV0()

    val fetchTxt = bag / "fetch.txt"
    val existingFetchItem1 = FetchItem(lipsum1URL, 12L, bag.data / "sub" / "u")
    val existingFetchItem2 = FetchItem(lipsum2URL, 12L, bag.data / "sub" / "v")
    val existingFetchItem3 = FetchItem(lipsum3URL, 12L, bag.data / "y-old")
    val existingFetchItem4 = FetchItem(lipsum4URL, 12L, bag.data / "x")
    val newFetchItem = FetchItem(lipsum5URL, 12L, bag.data / "some-file1.txt")

    // initial assumptions
    bag.fetchFiles should contain only(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4,
    )
    fetchTxt.toJava should exist
    bag.baseDir should containInFetchOnly(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4
    )

    // changes + save
    bag.addFetchFile(newFetchItem.url, 12L, _ / "some-file1.txt")
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles should contain only(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4,
      newFetchItem,
    )
    fetchTxt.toJava should exist
    bag.baseDir should containInFetchOnly(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4,
      newFetchItem
    )

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "list the added fetch files in the payload manifests" in {
    assumeCanConnect(lipsum5URL)

    val bag = multipleManifestsBagV0()

    val newFetchItem = FetchItem(lipsum5URL, 12L, bag.data / "some-file.txt")

    // initial assumptions
    bag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key newFetchItem.file
    (bag / "manifest-sha1.txt").contentAsString should not include s"$lipsum5Sha1  ${ bag.baseDir.relativize(newFetchItem.file) }"

    bag.payloadManifests(ChecksumAlgorithm.SHA256) shouldNot contain key newFetchItem.file
    (bag / "manifest-sha256.txt").contentAsString should not include s"$lipsum5Sha256  ${ bag.baseDir.relativize(newFetchItem.file) }"

    // changes + save
    bag.addFetchFile(newFetchItem.url, 12L, _ / "some-file.txt")
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(newFetchItem.file -> lipsum5Sha1)
    (bag / "manifest-sha1.txt").contentAsString should include(s"$lipsum5Sha1  ${ bag.baseDir.relativize(newFetchItem.file) }")

    bag.payloadManifests(ChecksumAlgorithm.SHA256) should contain(newFetchItem.file -> lipsum5Sha256)
    (bag / "manifest-sha256.txt").contentAsString should include(s"$lipsum5Sha256  ${ bag.baseDir.relativize(newFetchItem.file) }")

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "list fetch.txt in all tagmanifests" in {
    assumeCanConnect(lipsum5URL)

    val bag = multipleManifestsBagV0()

    val newFetchItem = FetchItem(lipsum5URL, 12L, bag.data / "some-file.txt")

    // initial assumptions
    bag.tagManifests(ChecksumAlgorithm.SHA1) shouldNot contain key (bag.baseDir / "fetch.txt")
    (bag / "tagmanifest-sha1.txt").contentAsString should not include "fetch.txt"

    bag.tagManifests(ChecksumAlgorithm.SHA256) shouldNot contain key (bag.baseDir / "fetch.txt")
    (bag / "tagmanifest-sha256.txt").contentAsString should not include "fetch.txt"

    // changes + save
    bag.addFetchFile(newFetchItem.url, 12L, _ / "some-file.txt")
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.tagManifests(ChecksumAlgorithm.SHA1) should contain key (bag.baseDir / "fetch.txt")
    (bag / "tagmanifest-sha1.txt").contentAsString should include("fetch.txt")

    bag.tagManifests(ChecksumAlgorithm.SHA256) should contain key (bag.baseDir / "fetch.txt")
    (bag / "tagmanifest-sha256.txt").contentAsString should include("fetch.txt")

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "delete fetch.txt when there were fetch files previously, but now they're gone" in {
    val bag = fetchBagV0()

    val fetchTxt = bag / "fetch.txt"
    val existingFetchItem1 = FetchItem(lipsum1URL, 12L, bag.data / "sub" / "u")
    val existingFetchItem2 = FetchItem(lipsum2URL, 12L, bag.data / "sub" / "v")
    val existingFetchItem3 = FetchItem(lipsum3URL, 12L, bag.data / "y-old")
    val existingFetchItem4 = FetchItem(lipsum4URL, 12L, bag.data / "x")

    // initial assumptions
    bag.fetchFiles should contain only(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4,
    )
    fetchTxt.toJava should exist
    bag.baseDir should containInFetchOnly(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4
    )

    // changes + save
    bag.removeFetchByURL(existingFetchItem1.url)
      .flatMap(_.removeFetchByURL(existingFetchItem2.url))
      .flatMap(_.removeFetchByURL(existingFetchItem3.url))
      .flatMap(_.removeFetchByURL(existingFetchItem4.url))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles shouldBe empty
    fetchTxt.toJava shouldNot exist

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "update existing manifest-<alg>.txt files" in {
    val bag = simpleBagV0()

    val u = bag.data / "sub" / "u"
    val v = bag.data / "sub" / "v"
    val w = bag.data / "sub" / "w"
    val x = bag.data / "x"
    val y = bag.data / "y"
    val z = bag.data / "z"
    val abc = bag.data / "abc.txt"

    val sha1Manifest = bag / "manifest-sha1.txt"

    // initial assumptions
    bag.payloadManifests.keySet should contain only ChecksumAlgorithm.SHA1
    bag.payloadManifests should containInManifestOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    sha1Manifest.contentAsString should not include bag.relativize(abc).toString

    // changes + save
    val newFile = testDir / "xxx.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    bag.addPayloadFile(newFile)(_ / "abc.txt")
      .flatMap(_.removePayloadFile(_ / "y"))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, abc, z)
    sha1Manifest.contentAsString should not include bag.relativize(y).toString

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "create new manifest-<alg>.txt files for not yet existing manifests" in {
    val bag = simpleBagV0()

    val u = bag.data / "sub" / "u"
    val v = bag.data / "sub" / "v"
    val w = bag.data / "sub" / "w"
    val x = bag.data / "x"
    val y = bag.data / "y"
    val z = bag.data / "z"
    val abc = bag.data / "abc.txt"

    val sha1Manifest = bag / "manifest-sha1.txt"
    val sha256Manifest = bag / "manifest-sha256.txt"

    // initial assumptions
    sha1Manifest.toJava should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    sha1Manifest.contentAsString should not include bag.relativize(abc).toString
    sha256Manifest.toJava shouldNot exist

    // changes + save
    val newFile = testDir / "xxx.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    bag.addPayloadManifestAlgorithm(ChecksumAlgorithm.SHA256)
      .flatMap(_.addPayloadFile(newFile)(_ / "abc.txt"))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    sha256Manifest.toJava should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z, abc)
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA256)(u, v, w, x, y, z, abc)

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "delete old manifest-<alg>.txt files that are no longer in use" in {
    val bag = multipleManifestsBagV0()

    val u = bag.data / "sub" / "u"
    val v = bag.data / "sub" / "v"
    val w = bag.data / "sub" / "w"
    val x = bag.data / "x"
    val y = bag.data / "y"
    val z = bag.data / "z"

    val sha1Manifest = bag / "manifest-sha1.txt"
    val sha256Manifest = bag / "manifest-sha256.txt"

    // initial assumptions
    sha1Manifest.toJava should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)

    sha256Manifest.toJava should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA256)(u, v, w, x, y, z)

    // changes + save
    bag.removePayloadManifestAlgorithm(ChecksumAlgorithm.SHA256)
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    sha1Manifest.toJava should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    sha256Manifest.toJava shouldNot exist

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "recompute the tag manifests and save them" in {
    val bag = multipleManifestsBagV0()

    val x = bag.data / "x"
    val y = bag.data / "y"
    val z = bag.data / "z"
    val u = bag.data / "sub" / "u"
    val v = bag.data / "sub" / "v"
    val w = bag.data / "sub" / "w"
    val datasetXml = bag / "metadata" / "dataset.xml"
    val filesXml = bag / "metadata" / "files.xml"
    val bagit = bag / "bagit.txt"
    val bagInfo = bag / "bag-info.txt"
    val manifestSha1 = bag / "manifest-sha1.txt"
    val manifestSha256 = bag / "manifest-sha256.txt"

    val newFileSrc = testDir / "newFile.txt" createIfNotExists() writeText lipsum(5)
    val newFile = bag.data / "lipsum5.txt"

    val bagInfoSha1 = bagInfo.sha1.toLowerCase
    val manifestSha1Sha1 = manifestSha1.sha1.toLowerCase

    // initial assumptions
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(x, y, z, u, v, w)
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA256)(x, y, z, u, v, w)
    bag.baseDir should containInTagManifestFileOnly(ChecksumAlgorithm.SHA1)(
      bagit, bagInfo, manifestSha1, manifestSha256, datasetXml, filesXml
    )
    bag.baseDir should containInTagManifestFileOnly(ChecksumAlgorithm.SHA256)(
      bagit, bagInfo, manifestSha1, manifestSha256, datasetXml, filesXml
    )

    // changes + save
    bag.addPayloadFile(newFileSrc)(_ => newFile)
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(x, y, z, u, v, w, newFile)
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA256)(x, y, z, u, v, w, newFile)
    bag.baseDir should containInTagManifestFileOnly(ChecksumAlgorithm.SHA1)(
      bagit, bagInfo, manifestSha1, manifestSha256, datasetXml, filesXml
    )
    bag.baseDir should containInTagManifestFileOnly(ChecksumAlgorithm.SHA256)(
      bagit, bagInfo, manifestSha1, manifestSha256, datasetXml, filesXml
    )

    bagInfoSha1 should not be bagInfo.sha1.toLowerCase
    manifestSha1Sha1 should not be manifestSha1.sha1.toLowerCase

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "create new tagmanifest-<alg>.txt files for not yet existing tagmanifests" in {
    val bag = simpleBagV0()

    val newFileSrc = testDir / "newFile.txt" createIfNotExists() writeText lipsum(5)
    val newFile = bag / "metadata" / "lipsum5.txt"

    // initial assumptions
    bag.glob("tagmanifest-*.txt").map(_.name).toSeq should contain only "tagmanifest-sha1.txt"

    // changes + save
    bag.addTagManifestAlgorithm(ChecksumAlgorithm.MD5)
      .flatMap(_.addTagFile(newFileSrc)(_ => newFile))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.glob("tagmanifest-*.txt").map(_.name).toSeq should contain only(
      "tagmanifest-md5.txt",
      "tagmanifest-sha1.txt",
    )

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "fail when no payload manifest is present in the bag" in {
    val bag = simpleBagV0()
    val algorithm = ChecksumAlgorithm.SHA1

    // initial assumptions
    bag.payloadManifestAlgorithms should contain only algorithm

    // changes
    inside(bag.removePayloadManifestAlgorithm(algorithm)) {
      case Success(resultBag) =>
        // expected results (1)
        resultBag.payloadManifestAlgorithms shouldBe empty

        // save
        inside(resultBag.save()) {
          case Failure(e: IllegalStateException) =>
            // expected results (2)
            e should have message "bag must contain at least one payload manifest"
        }
    }
  }
}
