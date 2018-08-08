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

import java.io.IOException
import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException, Paths }

import better.files.File
import nl.knaw.dans.bag.fixtures.{ BagMatchers, Lipsum, TestBags, TestSupportFixture }
import nl.knaw.dans.bag.{ ChecksumAlgorithm, RelativePath }

import scala.language.existentials
import scala.util.{ Failure, Success, Try }

class ManifestSpec extends TestSupportFixture with TestBags with BagMatchers with Lipsum {

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

  def addPayloadFile(addPayloadFile: DansV0Bag => File => RelativePath => Try[DansV0Bag]): Unit = {
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

  "addPayloadFile with InputStream and RelativePath" should behave like addPayloadFile(
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

  "addPayloadFile with InputStream and java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
    val relativeDest: RelativePath = _ / "path" / "to" / "file-copy.txt"
    val dest = relativeDest(bag.data)

    dest.toJava shouldNot exist

    file.inputStream()(bag.addPayloadFile(_, Paths.get("path/to/file-copy.txt"))) shouldBe a[Success[_]]

    dest.toJava should exist
    dest.contentAsString shouldBe file.contentAsString
    dest.sha1 shouldBe file.sha1
  }

  "addPayloadFile with File and RelativePath" should behave like addPayloadFile(_.addPayloadFile)

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

  "addPayloadFile with File and java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
    val relativeDest: RelativePath = _ / "path" / "to" / "file-copy.txt"
    val dest = relativeDest(bag.data)

    dest.toJava shouldNot exist

    bag.addPayloadFile(file, Paths.get("path/to/file-copy.txt")) shouldBe a[Success[_]]

    dest.toJava should exist
    dest.contentAsString shouldBe file.contentAsString
    dest.sha1 shouldBe file.sha1
  }

  "removePayloadFile with Relativepath" should "remove a payload file from the bag" in {
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

  "removePayloadFile with java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = bag.data / "sub" / "u"

    file.toJava should exist

    bag.removePayloadFile(Paths.get("sub/u")) shouldBe a[Success[_]]
    file.toJava shouldNot exist
    file.parent.toJava should exist
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

  def addTagFile(addTagFile: DansV0Bag => File => RelativePath => Try[DansV0Bag]): Unit = {
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

  "addTagFile with InputStream and RelativePath" should behave like addTagFile(
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

  "addTagFile with InputStream and java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
    val relativeDest: RelativePath = _ / "metadata" / "temp" / "file-copy.txt"
    val dest = relativeDest(bag)

    dest.toJava shouldNot exist

    file.inputStream()(bag.addTagFile(_, Paths.get("metadata/temp/file-copy.txt"))) shouldBe a[Success[_]]

    dest.toJava should exist
    dest.contentAsString shouldBe file.contentAsString
    dest.sha1 shouldBe file.sha1
  }

  "addTagFile with File and RelativePath" should behave like addTagFile(_.addTagFile)

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

  "addTagFile with File and java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
    val relativeDest: RelativePath = _ / "metadata" / "temp" / "file-copy.txt"
    val dest = relativeDest(bag)

    dest.toJava shouldNot exist

    bag.addTagFile(file, Paths.get("metadata/temp/file-copy.txt")) shouldBe a[Success[_]]

    dest.toJava should exist
    dest.contentAsString shouldBe file.contentAsString
    dest.sha1 shouldBe file.sha1
  }

  "removeTagFile with RelativePath" should "remove a tag file from the bag" in {
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

  it should "fail when the file to remove is the bag directory itself" in pendingUntilFixed {
    val bag = simpleBagV0()

    bag.toJava should exist
    bag.isChildOf(bag) shouldBe false // TODO why does this actually return true??? - https://github.com/pathikrit/better-files/issues/247

    inside(bag.removeTagFile(x => x)) {
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

  "removeTagFile with java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = simpleBagV0()
    val file = bag / "metadata" / "dataset.xml"

    file.toJava should exist

    bag.removeTagFile(Paths.get("metadata/dataset.xml")) shouldBe a[Success[_]]
    file.toJava shouldNot exist
    file.parent shouldBe bag / "metadata"
    file.parent.toJava should exist
  }
}
