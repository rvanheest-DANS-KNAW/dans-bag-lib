/**
 * Copyright (C) 2018 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.bag.v0

import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException }
import java.util.UUID

import better.files.File
import nl.knaw.dans.bag.ChecksumAlgorithm
import nl.knaw.dans.bag.ChecksumAlgorithm.ChecksumAlgorithm
import nl.knaw.dans.bag.fixtures._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.language.existentials
import scala.util.{ Failure, Success }

class FactoryMethodsSpec extends TestSupportFixture with TestBags with BagMatchers with Lipsum {

  "create empty" should "create a bag on the file system with an empty data directory" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    baseDir.toJava shouldNot exist

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.toJava should exist
    (baseDir / "data").toJava should exist
    (baseDir / "data").listRecursively.toList shouldBe empty
  }

  it should "create parent directories if they do not yet exist" in {
    val baseDir = testDir / "path" / "to" / "an" / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)
    val bagInfo = Map(
      "Is-Version-Of" -> Seq(s"urn:uuid:${ UUID.randomUUID() }")
    )

    baseDir.toJava shouldNot exist

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.toJava should exist
  }

  it should "create a bag-info.txt file if the given Map is empty, " +
    "containing only the Payload-Oxum and Bagging-Date" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set(ChecksumAlgorithm.SHA1)

    DansV0Bag.empty(baseDir, algorithms) shouldBe a[Success[_]]

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

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.empty(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.empty(baseDir, algorithms) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == baseDir.toString =>
    }
  }

  it should "fail when no algorithms are provided" in {
    val baseDir = testDir / "emptyTestBag"
    val algorithms = Set.empty[ChecksumAlgorithm]

    DansV0Bag.empty(baseDir, algorithms) should matchPattern {
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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    val file1Data = baseDir / "data" / baseDir.relativize(file1).toString
    val file2Data = baseDir / "data" / baseDir.relativize(file2).toString

    baseDir.toJava should exist
    (baseDir / "data").toJava should exist
    (baseDir / "data").listRecursively.filter(_.isRegularFile).toList should contain only(file1Data, file2Data)
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

    DansV0Bag.createFromData(baseDir, algorithms) shouldBe a[Success[_]]

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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

    baseDir.glob("manifest-*.txt").map(_.name).toList should contain only(
      "manifest-sha1.txt",
      "manifest-sha512.txt",
    )

    forEvery(algorithms)(algorithm => {
      baseDir should containInPayloadManifestFileOnly(algorithm)(
        baseDir / "data" / baseDir.relativize(file1).toString,
        baseDir / "data" / baseDir.relativize(file2).toString
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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) shouldBe a[Success[_]]

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

    DansV0Bag.createFromData(baseDir, algorithms, bagInfo) should matchPattern {
      case Failure(e: NoSuchFileException) if baseDir.toString == e.getMessage =>
    }
  }

  it should "fail when no algorithms are provided" in {
    val (baseDir, _, _) = createDataDir()
    val algorithms = Set.empty[ChecksumAlgorithm]

    DansV0Bag.createFromData(baseDir, algorithms) should matchPattern {
      case Failure(_: IllegalArgumentException) =>
    }
  }

  "read bag" should "load a bag located in the given directory into the object structure for a Bag" in {
    DansV0Bag.read(simpleBagDirV0) should matchPattern { case Success(_: DansV0Bag) => }
  }

  it should "fail when the directory does not exist" in {
    DansV0Bag.read(testDir / "non" / "existing" / "directory") should matchPattern {
      case Failure(_: NoSuchFileException) =>
    }
  }

  it should "fail when the directory does not represent a valid bag" in {
    val (baseDir, _, _) = createDataDir()
    DansV0Bag.read(baseDir) should matchPattern {
      case Failure(e: NoSuchFileException) if e.getMessage == (baseDir / "bagit.txt").toString =>
    }
  }
}
