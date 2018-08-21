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

import nl.knaw.dans.bag.ChecksumAlgorithm
import nl.knaw.dans.bag.fixtures.{ FetchFileMetadata, TestBags, TestSupportFixture }

class ValidationSpec extends TestSupportFixture with TestBags with FetchFileMetadata {

  "isComplete" should "succeed on a complete bag" in {
    val bag = simpleBagV0()

    (bag / "bagit.txt") should exist
    bag.data should exist
    bag.glob("manifest-*.txt").toList should not be empty
    bag.fetchFiles shouldBe empty

    bag.isComplete shouldBe 'right
  }

  it should "succeed on a complete bag with resolved fetch files" in {
    val bag = fetchBagV0()
    bag.data / "sub" createDirectory()
    lipsum1File.copyTo(bag.data / "sub" / "u")
    lipsum2File.copyTo(bag.data / "sub" / "v")
    lipsum3File.copyTo(bag.data / "y-old")
    lipsum4File.copyTo(bag.data / "x")

    (bag / "bagit.txt") should exist
    bag.data should exist
    bag.glob("manifest-*.txt").toList should not be empty
    every(bag.fetchFiles.map(_.file)) should exist

    bag.isComplete shouldBe 'right
  }

  it should "fail when bagit.txt does not exists" in {
    val bag = simpleBagV0()

    bag / "bagit.txt" delete()

    bag.isComplete.left.value shouldBe s"File [${ bag / "bagit.txt" }] should exist but it doesn't!"
  }

  it should "fail when no data/ directory is present" in pendingUntilFixed { // TODO https://github.com/LibraryOfCongress/bagit-java/issues/123
    val bag = simpleBagV0()

    bag.data.delete()

    bag.isComplete.left.value shouldBe s"File [${ bag.data }] should exist but it doesn't!"
  }

  it should "fail when no payload manifest exists" in {
    val bag = multipleManifestsBagV0()

    bag.glob("manifest-*.txt").foreach(_.delete())

    bag.isComplete.left.value shouldBe "Bag does not contain a payload manifest file!"
  }

  it should "fail when not all fetch files are resolved" in {
    val bag = fetchBagV0()

    bag.isComplete.left.value shouldBe s"Fetch item [$lipsum1URL 12 ${ bag.data / "sub" / "u" }] has not been fetched!"
  }

  it should "fail when not all files in the payload manifests are present" in {
    val bag = simpleBagV0()
    val x = bag.data / "x"

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key x
    }

    x.delete()

    bag.isComplete.left.value shouldBe s"Manifest(s) contains file(s) [$x] but they don't exist!"
  }

  it should "fail when not all files in the tag manifests are present" in {
    val bag = simpleBagV0()
    val bagInfo = bag / "bag-info.txt"

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bagInfo
    }

    bagInfo.delete()

    bag.isComplete.left.value shouldBe s"Manifest(s) contains file(s) [$bagInfo] but they don't exist!"
  }

  it should "fail when not all files are listed in all payload manifests" in {
    val bagDir = multipleManifestsBagDirV0
    bagDir / "manifest-sha1.txt" writeText ""

    val bag = multipleManifestsBagV0().withBagitVersion(1, 0)

    bag.payloadManifests(ChecksumAlgorithm.SHA1) shouldBe empty

    bag.isComplete.left.value should (
      startWith("File [") and
        endWith("] is in the payload directory but isn't listed in manifest manifest-sha1.txt!")
      )
  }

  "isValid" should "succeed on a valid bag" in {
    val bag = simpleBagV0()

    // check isComplete verifications
    (bag / "bagit.txt") should exist
    bag.data should exist
    bag.glob("manifest-*.txt").toList should not be empty
    bag.fetchFiles shouldBe empty

    // check payload manifest verification
    forEvery(bag.payloadManifests) {
      case (algorithm, manifest) =>
        forEvery(manifest) {
          case (file, checksum) =>
            file.checksum(algorithm).toLowerCase shouldBe checksum
        }
    }

    // check tag manifest verification
    forEvery(bag.tagManifests) {
      case (algorithm, manifest) =>
        forEvery(manifest) {
          case (file, checksum) =>
            file.checksum(algorithm).toLowerCase shouldBe checksum
        }
    }

    bag.isValid shouldBe 'right
  }

  it should "succeed on a valid bag with resolved fetch files" in {
    val bag = fetchBagV0()
    bag.data / "sub" createDirectory()
    lipsum1File.copyTo(bag.data / "sub" / "u")
    lipsum2File.copyTo(bag.data / "sub" / "v")
    lipsum3File.copyTo(bag.data / "y-old")
    lipsum4File.copyTo(bag.data / "x")

    // check isComplete verifications
    (bag / "bagit.txt") should exist
    bag.data should exist
    bag.glob("manifest-*.txt").toList should not be empty
    every(bag.fetchFiles.map(_.file)) should exist

    // check payload manifest verification
    forEvery(bag.payloadManifests) {
      case (algorithm, manifest) =>
        forEvery(manifest) {
          case (file, checksum) =>
            file.checksum(algorithm).toLowerCase shouldBe checksum
        }
    }

    // check tag manifest verification
    forEvery(bag.tagManifests) {
      case (algorithm, manifest) =>
        forEvery(manifest) {
          case (file, checksum) =>
            file.checksum(algorithm).toLowerCase shouldBe checksum
        }
    }

    bag.isValid shouldBe 'right
  }

  it should "fail when bagit.txt does not exists" in {
    val bag = simpleBagV0()

    bag / "bagit.txt" delete()

    bag.isValid.left.value shouldBe s"File [${ bag / "bagit.txt" }] should exist but it doesn't!"
  }

  it should "fail when no data/ directory is present" in pendingUntilFixed { // TODO https://github.com/LibraryOfCongress/bagit-java/issues/123
    val bag = simpleBagV0()

    bag.data.delete()

    bag.isValid.left.value shouldBe s"File [${ bag.data }] should exist but it doesn't!"
  }

  it should "fail when no payload manifest exists" in {
    val bag = multipleManifestsBagV0()

    bag.glob("manifest-*.txt").foreach(_.delete())

    bag.isValid.left.value shouldBe "Bag does not contain a payload manifest file!"
  }

  it should "fail when not all fetch files are resolved" in {
    val bag = fetchBagV0()

    bag.isValid.left.value shouldBe s"Fetch item [$lipsum1URL 12 ${ bag.data / "sub" / "u" }] has not been fetched!"
  }

  it should "fail when not all files in the payload manifests are present" in {
    val bag = simpleBagV0()
    val x = bag.data / "x"

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key x
    }

    x.delete()

    bag.isValid.left.value shouldBe s"Manifest(s) contains file(s) [$x] but they don't exist!"
  }

  it should "fail when not all files in the tag manifests are present" in {
    val bag = simpleBagV0()
    val bagInfo = bag / "bag-info.txt"

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bagInfo
    }

    bagInfo.delete()

    bag.isValid.left.value shouldBe s"Manifest(s) contains file(s) [$bagInfo] but they don't exist!"
  }

  it should "fail when not all files are listed in all payload manifests" in {
    val bagDir = multipleManifestsBagDirV0
    bagDir / "manifest-sha1.txt" writeText ""

    val bag = multipleManifestsBagV0().withBagitVersion(1, 0)

    bag.payloadManifests(ChecksumAlgorithm.SHA1) shouldBe empty

    bag.isValid.left.value should (
      startWith("File [") and
        endWith("] is in the payload directory but isn't listed in manifest manifest-sha1.txt!")
      )
  }

  it should "fail when the bag contains an invalid payload checksum" in {
    val bag = simpleBagV0()
    val x = bag.data / "x"
    val oldSha1 = x.sha1.toLowerCase

    x writeText "this causes an invalid checksum for the file"
    val newSha1 = x.sha1.toLowerCase

    bag.isValid.left.value shouldBe s"File [$x] is suppose to have a [SHA-1] hash of [$oldSha1] but was computed [$newSha1]."
  }

  it should "fail when the bag contains an invalid tag checksum" in {
    val bag = simpleBagV0()
    val datasetxml = bag / "metadata" / "dataset.xml"
    val oldSha1 = datasetxml.sha1.toLowerCase

    datasetxml writeText "this causes an invalid checksum for the file"
    val newSha1 = datasetxml.sha1.toLowerCase

    bag.isValid.left.value shouldBe s"File [$datasetxml] is suppose to have a [SHA-1] hash of [$oldSha1] but was computed [$newSha1]."
  }
}
