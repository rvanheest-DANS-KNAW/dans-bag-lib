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

import java.io.{ ByteArrayInputStream, IOException }
import java.nio.file.spi.FileSystemProvider
import java.nio.file.{ FileAlreadyExistsException, _ }

import better.files.File
import nl.knaw.dans.bag.ChecksumAlgorithm
import nl.knaw.dans.bag.ImportOption.ATOMIC_MOVE
import nl.knaw.dans.bag.fixtures.{ BagMatchers, Lipsum, TestBags, TestSupportFixture }

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

  def addPayloadFile(addPayloadFile: DansV0Bag => (File, Path) => Try[DansV0Bag]): Unit = {
    it should "copy the new file into the bag" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("path/to/file-copy.txt")
      val dest = bag.data / relativeDest.toString

      dest shouldNot exist

      addPayloadFile(bag)(file, relativeDest) shouldBe a[Success[_]]

      dest should exist
      dest.contentAsString shouldBe file.contentAsString
      dest.sha1 shouldBe file.sha1
    }

    it should "add the checksum for all algorithms in the bag to the payload manifest" in {
      val bag = multipleManifestsBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("path/to/file-copy.txt")
      val dest = bag.data / relativeDest.toString

      bag.payloadManifestAlgorithms should contain only(
        ChecksumAlgorithm.SHA1,
        ChecksumAlgorithm.SHA256,
      )

      inside(addPayloadFile(bag)(file, relativeDest)) {
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
      val relativeDest = Paths.get("../../path/to/file-copy.txt")
      val dest = bag.data / relativeDest.toString

      dest shouldNot exist

      inside(addPayloadFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"pathInData '$dest' is supposed to point to a file that is a child of the bag/data directory"
      }

      dest shouldNot exist
    }

    it should "fail when the destination already exists" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("../../path/to/file-copy.txt")
      val dest = (bag.data / relativeDest.toString) createIfNotExists (createParents = true) writeText lipsum(1)

      dest should exist

      inside(addPayloadFile(bag)(file, relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message dest.toString
      }

      dest should exist
      dest.contentAsString shouldBe lipsum(1)
    }

    it should "fail when the destination is already mentioned in the fetch file" in {
      val bag = fetchBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("x")
      val dest = bag.data / relativeDest.toString

      dest shouldNot exist
      bag.fetchFiles.map(_.file) contains dest
      bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest
      val sha1Before = bag.payloadManifests(ChecksumAlgorithm.SHA1)(dest)
      file.sha1 should not be sha1Before

      inside(addPayloadFile(bag)(file, relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message s"$dest: file already present in bag as a fetch file"

          dest shouldNot exist
          bag.fetchFiles.map(_.file) contains dest
          bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest

          val sha1After = bag.payloadManifests(ChecksumAlgorithm.SHA1)(dest)
          sha1Before shouldBe sha1After
      }
    }
  }

  "addPayloadFile with InputStream" should behave like addPayloadFile(
    bag => (file, path) => file.inputStream()(bag.addPayloadFile(_, path))
  )

  it should "fail when the given file is a directory" in {
    val bag = simpleBagV0()
    val newDir = testDir / "newDir" createDirectories()
    newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    newDir / "sub" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    newDir / "sub" / "subsub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val relativeDest = Paths.get("path/to/newDir")

    inside(newDir.inputStream()(bag.addPayloadFile(_, relativeDest))) {
      case Failure(e: IOException) =>
        e should have message "Is a directory"
    }
  }

  it should "fail if the destination is not inside the bag/data directory" in {
    val bag = fetchBagV0()
    val path = bag.data.parent / "file"
    val inputStream = new ByteArrayInputStream("Lorum ipsum".getBytes())

    bag.addPayloadFile(inputStream, Paths.get("../file")) should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage ==
        s"pathInData '$path' is supposed to point to a file that is a child of the bag/data directory" =>
    }
  }

  it should "fail if the destination exists" in {
    val bag = simpleBagV0()
    val inputStream = new ByteArrayInputStream("Lorum ipsum".getBytes())
    bag.addPayloadFile(inputStream, Paths.get("sub/u")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "sub" / "u").toString() =>
    }
  }

  it should "fail if the destination is present as fetch file" in {
    val bag = fetchBagV0()
    val inputStream = new ByteArrayInputStream("Lorum ipsum".getBytes())
    bag.addPayloadFile(inputStream, Paths.get("sub/u")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage ==
        s"${ bag.data }/sub/u: file already present in bag as a fetch file" =>
    }
  }

  "addPayloadFile with File" should "fail if the destination is not inside the bag/data directory" in {
    val bag = fetchBagV0()
    val path = bag.data.parent / "file"
    val stagedFile = (testDir / "some.file").createFile()

    bag.addPayloadFile(stagedFile, Paths.get("../file")) should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage ==
        s"pathInData '$path' is supposed to point to a file that is a child of the bag/data directory" =>
    }
  }

  it should "fail if the destination exists" in {
    val bag = simpleBagV0()
    val stagedFile = (testDir / "some.file").createFile()
    bag.addPayloadFile(stagedFile, Paths.get("sub/u")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "sub" / "u").toString() =>
    }
  }

  it should "fail if the destination is present as fetch file" in {
    val bag = fetchBagV0()
    val stagedFile = (testDir / "some.file").createFile()
    bag.addPayloadFile(stagedFile, Paths.get("sub/u")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage ==
        s"${ bag.data }/sub/u: file already present in bag as a fetch file" =>
    }
  }

  "addPayloadFile with File and COPY" should behave like addPayloadFile(_.addPayloadFile)

  it should "recursively add the files and folders in the directory to the bag" in {
    val bag = multipleManifestsBagV0()
    val newDir = testDir / "newDir" createDirectories()
    val file1 = newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    val file2 = newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    val file3 = newDir / "sub1" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    val file4 = newDir / "sub1" / "sub1sub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val file5 = newDir / "sub2" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    val relativeDest = Paths.get("path/to/newDir")
    val dest = bag.data / relativeDest.toString

    inside(bag.addPayloadFile(newDir, relativeDest)) {
      case Success(resultBag) =>
        val files = Set(file1, file2, file3, file4, file5)
          .map(file => dest / newDir.relativize(file).toString)

        forEvery(files)(file => {
          file should exist

          forEvery(List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256))(algo => {
            resultBag.payloadManifests(algo) should contain key file
            resultBag.payloadManifests(algo)(file) shouldBe file.checksum(algo).toLowerCase
          })
        })
    }
  }

  "addPayloadFile with File and ATOMIC_MOVE" should "move the staged file" in {
    val bag = simpleBagV0()
    val stagedFile = testDir / "some.file" createFile() writeText "this file is supposed to be moved into the bag"
    val fileChecksumSha1 = stagedFile.sha1.toLowerCase

    inside(bag.addPayloadFile(stagedFile, Paths.get("new/file"))(ATOMIC_MOVE)) {
      case Success(resultBag) =>
        val newFilePath = resultBag.data / "new" / "file"

        stagedFile shouldNot exist
        newFilePath should exist

        resultBag.payloadManifests.keySet should contain only ChecksumAlgorithm.SHA1
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(newFilePath -> fileChecksumSha1)
    }
  }

  "addPayloadFile" should "move a directory with ATOMIC_MOVE" in {
    val bag = simpleBagV0()
    val stagedDir = (testDir / "some-dir").createDirectories()
    val stagedFile = stagedDir / "some.txt"
    stagedFile createFile() writeText "this file is supposed to be moved into the bag"
    val fileChecksumSha1 = stagedFile.sha1.toLowerCase

    inside(bag.addPayloadFile(stagedDir, Paths.get("sub2"))(ATOMIC_MOVE)) {
      case Success(resultBag) =>
        val newFilePath = resultBag.data / "sub2" / "some.txt"

        stagedDir shouldNot exist
        newFilePath should exist

        resultBag.payloadManifests.keySet should contain only ChecksumAlgorithm.SHA1
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(newFilePath -> fileChecksumSha1)
    }
  }

  it should "copy a directory to an empty data dir" in {
    val bag = DansV0Bag.empty(testDir / "bag")
    val stagedDir = (testDir / "some-dir").createDirectories()
    val stagedFile = stagedDir / "some.txt" createFile() writeText
      "this is content for a file that is supposed to be moved into the bag"
    val fileChecksumSha1 = stagedFile.sha1.toLowerCase

    inside(bag.addPayloadFile(stagedDir, Paths.get("."))) {
      case Success(resultBag) =>
        val newFilePath = resultBag.data / "some.txt"

        stagedDir should exist
        newFilePath should exist

        resultBag.payloadManifests.keySet should contain only ChecksumAlgorithm.SHA1
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(newFilePath -> fileChecksumSha1)
    }
  }

  it should "not copy a directory to a data dir with content" in {
    val bag = simpleBagV0()
    val stagedDir = (testDir / "some-dir").createDirectories()

    bag.addPayloadFile(stagedDir, Paths.get(".")) should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage ==
        "The data directory must be empty to receive content of a directory." =>
    }
  }

  it should "not copy a file to the data dir" in {
    val bag = DansV0Bag.empty(testDir / "bag")
    val stagedDir = (testDir / "some-file").createFile()

    bag.addPayloadFile(stagedDir, Paths.get(".")) should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage ==
        s"The data directory can only receive content of a directory, got: $stagedDir" =>
    }
  }

  it should "fail in case of atomic move across different providers" in {
    // File has a private constructor so we can't mock it
    // that doesn't get us past the check for a regular file
    // thus we can't test an atomic move across different mounts
    val bag = fetchBagV0()
    val mockedSrcPath = mock[Path]
    val mockedFileSystem = mock[FileSystem]
    val mockedFileSystemProvider = mock[FileSystemProvider]

    mockedSrcPath.toAbsolutePath _ expects() returning mockedSrcPath
    mockedSrcPath.normalize _ expects() returning mockedSrcPath
    mockedSrcPath.getFileSystem _ expects() returning mockedFileSystem
    mockedFileSystem.provider _ expects() returning mockedFileSystemProvider

    inside(bag.addPayloadFile(File(mockedSrcPath), Paths.get("new/file"))(ATOMIC_MOVE)) {
      case Failure(e: AtomicMoveNotSupportedException) =>
        val expectedPath = bag.data / "new" / "file"
        e should have message s"$mockedSrcPath -> $expectedPath: Different providers, atomic move cannot take place"
    }
  }

  "removePayloadFile" should "remove a payload file from the bag" in {
    val bag = simpleBagV0()
    val file = bag.data / "sub" / "u"

    file should exist

    bag.removePayloadFile(Paths.get("sub/u")) shouldBe a[Success[_]]
    file shouldNot exist
    file.parent should exist
  }

  it should "remove the removed payload file from all manifests" in {
    val bag = multipleManifestsBagV0()
    val file = bag.data / "x"

    bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key file
    bag.payloadManifests(ChecksumAlgorithm.SHA256) should contain key file

    inside(bag.removePayloadFile(Paths.get("x"))) {
      case Success(resultBag) =>
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key file
        resultBag.payloadManifests(ChecksumAlgorithm.SHA256) shouldNot contain key file
    }
  }

  it should "clean up empty directories after the last payload file in a directory is removed" in {
    val bag = simpleBagV0()
    val file = testDir / "a.txt" createIfNotExists (createParents = true) writeText "content of file a"
    val destination = bag.data / "path" / "to" / "file" / "a.txt"

    bag.addPayloadFile(file, Paths.get("path/to/file/a.txt")) shouldBe a[Success[_]]

    destination should exist
    bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key destination

    inside(bag.removePayloadFile(Paths.get("path/to/file/a.txt"))) {
      case Success(resultBag) =>
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key destination

        destination shouldNot exist
        destination.parent shouldNot exist
        destination.parent.parent shouldNot exist
        destination.parent.parent.parent shouldNot exist
        destination.parent.parent.parent.parent should exist
        destination.parent.parent.parent.parent shouldBe resultBag.data
    }
  }

  it should "keep an empty bag/data directory if all files are removed from the bag" in {
    val bag = simpleBagV0()

    val result = bag.removePayloadFile(Paths.get("x"))
      .flatMap(_.removePayloadFile(Paths.get("y")))
      .flatMap(_.removePayloadFile(Paths.get("z")))
      .flatMap(_.removePayloadFile(Paths.get("sub/u")))
      .flatMap(_.removePayloadFile(Paths.get("sub/v")))
      .flatMap(_.removePayloadFile(Paths.get("sub/w")))

    inside(result) {
      case Success(resultBag) =>
        resultBag.data should exist
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

    val result = bag.removePayloadFile(Paths.get("x"))
      .flatMap(_.removePayloadFile(Paths.get("y")))
      .flatMap(_.removePayloadFile(Paths.get("z")))
      .flatMap(_.removePayloadFile(Paths.get("sub/u")))
      .flatMap(_.removePayloadFile(Paths.get("sub/v")))
      .flatMap(_.removePayloadFile(Paths.get("sub/w")))

    inside(result) {
      case Success(resultBag) =>
        forEvery(List(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.SHA256))(algo => {
          resultBag.payloadManifests should contain key algo

          resultBag.payloadManifests(algo) shouldBe empty
        })
    }
  }

  it should "fail when the file does not exist" in {
    val bag = simpleBagV0()
    val file = bag.data / "doesnotexist.txt"

    file shouldNot exist

    inside(bag.removePayloadFile(Paths.get("doesnotexist.txt"))) {
      case Failure(e: NoSuchFileException) =>
        e should have message file.toString
    }
  }

  it should "fail when the file is not inside the bag/data directory" in {
    val bag = simpleBagV0()
    val file = bag / "bagit.txt"

    file should exist
    file.isChildOf(bag.data) shouldBe false

    inside(bag.removePayloadFile(Paths.get("../bagit.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"pathInData '$file' is supposed to point to a file that is a " +
          s"child of the bag/data directory"
    }
  }

  it should "fail when the file to be removed is a directory" in {
    val bag = simpleBagV0()
    val file = bag.data / "sub"

    file should exist
    file.isDirectory shouldBe true
    val checksumsBeforeCall = bag.payloadManifests

    inside(bag.removePayloadFile(Paths.get("sub"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove directory '$file'; you can only remove files"

        file should exist
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

  def addTagFile(addTagFile: DansV0Bag => (File, Path) => Try[DansV0Bag]): Unit = {
    it should "copy the new file into the bag" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("metadata/temp/file-copy.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest shouldNot exist

      addTagFile(bag)(file, relativeDest) shouldBe a[Success[_]]

      dest should exist
      dest.contentAsString shouldBe file.contentAsString
      dest.sha1 shouldBe file.sha1
    }

    it should "add the checksum for all algorithms in the bag to the tag manifests" in {
      val bag = multipleManifestsBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("metadata/temp/file-copy.txt")
      val dest = bag.baseDir / relativeDest.toString

      bag.tagManifestAlgorithms should contain only(
        ChecksumAlgorithm.SHA1,
        ChecksumAlgorithm.SHA256,
      )

      inside(addTagFile(bag)(file, relativeDest)) {
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
      val relativeDest = Paths.get("data/path/to/file-copy.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"cannot add a tag file like '$dest' to the bag/data directory"
      }

      dest shouldNot exist
    }

    it should "fail when the destination is outside the bag directory" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("../path/to/file-copy.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message s"cannot add a tag file like '$dest' to a place outside the bag directory"
      }

      dest shouldNot exist
    }

    it should "fail when the destination already exists" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("metadata/dataset.xml")
      val dest = bag.baseDir / relativeDest.toString

      dest should exist
      val oldContent = dest.contentAsString

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: FileAlreadyExistsException) =>
          e should have message dest.toString
      }

      dest should exist
      dest.contentAsString shouldBe oldContent
    }

    it should "fail when the destination points to bag/bag-info.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("bag-info.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest.delete()

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'bag-info.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest shouldNot exist
    }

    it should "fail when the destination points to bag/bagit.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("bagit.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest.delete()

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'bagit.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest shouldNot exist
    }

    it should "fail when the destination points to bag/fetch.txt, which was removed first" in {
      val bag = fetchBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("fetch.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest.delete()

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tag file 'fetch.txt' is controlled by the library itself; you cannot add a file to this location"
      }

      dest shouldNot exist
    }

    it should "fail when the destination points to bag/manifest-XXX.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("manifest-sha1.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest.delete()

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "manifest files are controlled by the library itself; you cannot add a file to this location"
      }

      dest shouldNot exist
    }

    it should "fail when the destination points to bag/tagmanifest-XXX.txt, which was removed first" in {
      val bag = simpleBagV0()
      val file = testDir / "file.txt" createIfNotExists() writeText lipsum(3)
      val relativeDest = Paths.get("tagmanifest-sha1.txt")
      val dest = bag.baseDir / relativeDest.toString

      dest.delete()

      dest shouldNot exist

      inside(addTagFile(bag)(file, relativeDest)) {
        case Failure(e: IllegalArgumentException) =>
          e should have message "tagmanifest files are controlled by the library itself; you cannot add a file to this location"
      }

      dest shouldNot exist
    }
  }

  "addTagFile with InputStream" should behave like addTagFile(
    bag => (file, relativeDest) => file.inputStream()(bag.addTagFile(_, relativeDest))
  )

  it should "fail when the given file is a directory" in {
    val bag = simpleBagV0()
    val newDir = testDir / "newDir" createDirectories()
    newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    newDir / "sub" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    newDir / "sub" / "subsub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val relativeDest = Paths.get("path/to/newDir")

    inside(newDir.inputStream()(bag.addTagFile(_, relativeDest))) {
      case Failure(e: IOException) =>
        e should have message "Is a directory"
    }
  }

  "addTagFile with File" should behave like addTagFile(_.addTagFile)

  it should "recursively add the files and folders in the directory to the bag" in {
    val bag = multipleManifestsBagV0()
    val newDir = testDir / "newDir" createDirectories()
    val file1 = newDir / "file1.txt" createIfNotExists() writeText lipsum(1)
    val file2 = newDir / "file2.txt" createIfNotExists() writeText lipsum(2)
    val file3 = newDir / "sub1" / "file3.txt" createIfNotExists (createParents = true) writeText lipsum(3)
    val file4 = newDir / "sub1" / "sub1sub" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(4)
    val file5 = newDir / "sub2" / "file4.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    val relativeDest = Paths.get("path/to/newDir")
    val dest = bag.baseDir / relativeDest.toString

    inside(bag.addTagFile(newDir, relativeDest)) {
      case Success(resultBag) =>
        val files = Set(file1, file2, file3, file4, file5)
          .map(file => dest / newDir.relativize(file).toString)

        forEvery(files)(file => {
          file should exist

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

    file should exist

    bag.removeTagFile(Paths.get("metadata/dataset.xml")) shouldBe a[Success[_]]
    file shouldNot exist
    file.parent shouldBe bag / "metadata"
    file.parent should exist
  }

  it should "remove the removed tag file from all manifests" in {
    val bag = multipleManifestsBagV0()
    val file = bag / "metadata" / "dataset.xml"

    bag.tagManifests(ChecksumAlgorithm.SHA1) should contain key file
    bag.tagManifests(ChecksumAlgorithm.SHA256) should contain key file

    inside(bag.removeTagFile(Paths.get("metadata/dataset.xml"))) {
      case Success(resultBag) =>
        resultBag.tagManifests(ChecksumAlgorithm.SHA1) shouldNot contain key file
        resultBag.tagManifests(ChecksumAlgorithm.SHA256) shouldNot contain key file
    }
  }

  it should "clean up empty directories after the last tag file in a directory is removed" in {
    val bag = simpleBagV0()
    val file = testDir / "a.txt" createIfNotExists (createParents = true) writeText "content of file a"
    val destination = bag / "path" / "to" / "file" / "a.txt"

    bag.addTagFile(file, Paths.get("path/to/file/a.txt")) shouldBe a[Success[_]]

    destination should exist
    bag.tagManifests(ChecksumAlgorithm.SHA1) should contain key destination

    inside(bag.removeTagFile(Paths.get("path/to/file/a.txt"))) {
      case Success(resultBag) =>
        resultBag.tagManifests(ChecksumAlgorithm.SHA1) shouldNot contain key destination

        destination shouldNot exist
        destination.parent shouldNot exist
        destination.parent.parent shouldNot exist
        destination.parent.parent.parent shouldNot exist
        destination.parent.parent.parent.parent shouldBe resultBag.baseDir
        destination.parent.parent.parent.parent should exist
    }
  }

  it should "fail when the file does not exist" in {
    val bag = simpleBagV0()
    val file = bag / "doesnotexist.txt"

    file shouldNot exist

    inside(bag.removeTagFile(Paths.get("doesnotexist.txt"))) {
      case Failure(e: NoSuchFileException) => e should have message file.toString
    }
  }

  it should "fail when the file is inside the bag/data directory" in {
    val bag = simpleBagV0()
    val file = bag.data / "x"

    file should exist
    file.isChildOf(bag.data) shouldBe true

    inside(bag.removeTagFile(Paths.get("data/x"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove '$file' since it is a child of the bag/data directory"
    }
  }

  it should "fail when the file is outside the bag directory" in {
    val bag = simpleBagV0()
    val file = bag / ".." / "temp.txt" createIfNotExists() writeText "content of temp.txt"

    file should exist
    file.isChildOf(bag) shouldBe false

    inside(bag.removeTagFile(Paths.get("../temp.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove '$file' since it is not a child of the bag directory"
    }
  }

  it should "fail when the file to remove is the bag directory itself" in {
    val bag = simpleBagV0()

    bag.baseDir should exist
    bag.isChildOf(bag) shouldBe false

    inside(bag.removeTagFile(Paths.get(""))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "cannot remove the whole bag"
    }
  }

  it should "fail when the file is the bag/bag-info.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "bag-info.txt"

    file should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("bag-info.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is the bag/bagit.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "bagit.txt"

    file should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("bagit.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is the bag/fetch.txt file" in {
    val bag = fetchBagV0()
    val file = bag / "fetch.txt"

    file should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("fetch.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove bag specific file '$file'"
    }
  }

  it should "fail when the file is a bag/manifest-XXX.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "manifest-sha1.txt"

    file should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("manifest-sha1.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove manifest file '$file'"
    }
  }

  it should "fail when the file is a bag/tagmanifest-XXX.txt file" in {
    val bag = simpleBagV0()
    val file = bag / "tagmanifest-sha1.txt"

    file should exist
    file.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("tagmanifest-sha1.txt"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove tagmanifest file '$file'"
    }
  }

  it should "fail when the file is a directory" in {
    val bag = simpleBagV0()
    val dir = bag / "metadata"

    dir should exist
    dir.isChildOf(bag) shouldBe true

    inside(bag.removeTagFile(Paths.get("metadata"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"cannot remove directory '$dir'; you can only remove files"
    }
  }
}
