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

import java.nio.charset.StandardCharsets
import java.util.UUID

import gov.loc.repository.bagit.conformance.{ BagLinter, BagitWarning }
import gov.loc.repository.bagit.domain.Version
import gov.loc.repository.bagit.verify.BagVerifier
import nl.knaw.dans.bag.fixtures._
import nl.knaw.dans.bag.{ ChecksumAlgorithm, FetchItem }
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.{ Failure, Success }
import scala.language.existentials

class SaveSpec extends TestSupportFixture
  with TestBags
  with BagMatchers
  with Lipsum
  with FetchFileMetadata
  with CanConnect {

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
      "Bag-Size" -> Seq("2.5 KB"),
      "Created" -> Seq("2017-01-16T14:35:00.888+01:00"),
      "Is-Version-Of" -> Seq(uuid),
    )
    bag.baseDir should containInBagInfoOnly(
      "Payload-Oxum" -> Seq("2592.7"),
      "Bagging-Date" -> Seq(today),
      "Bag-Size" -> Seq("2.5 KB"),
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
    fetchTxt shouldNot exist

    // (no) changes + save
    bag.save() shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles shouldBe empty
    fetchTxt shouldNot exist

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
    val fetchItem1 = FetchItem(lipsum1URL, lipsum1Size, bag.data / "some-file1.txt")
    val fetchItem2 = FetchItem(lipsum2URL, lipsum2Size, bag.data / "some-file2.txt")

    // initial assumptions
    bag.fetchFiles shouldBe empty
    fetchTxt shouldNot exist

    // changes + save
    bag.addFetchItem(lipsum1URL, _ / "some-file1.txt")
      .flatMap(_.addFetchItem(lipsum2URL, _ / "some-file2.txt"))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles should contain only(
      fetchItem1,
      fetchItem2,
    )
    fetchTxt should exist
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

  it should "save fetch.txt when there were already fetch files in the bag" in pendingUntilFixed { // TODO https://github.com/LibraryOfCongress/bagit-java/issues/117
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
    fetchTxt should exist
    bag.baseDir should containInFetchOnly(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4
    )

    // changes + save
    bag.addFetchItem(newFetchItem.url, _ / "some-file1.txt")
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles should contain only(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4,
      newFetchItem,
    )
    fetchTxt should exist
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
    bag.addFetchItem(newFetchItem.url, _ / "some-file.txt")
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
    bag.addFetchItem(newFetchItem.url, _ / "some-file.txt")
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
    fetchTxt should exist
    bag.baseDir should containInFetchOnly(
      existingFetchItem1,
      existingFetchItem2,
      existingFetchItem3,
      existingFetchItem4
    )

    // changes + save
    bag.removeFetchItem(existingFetchItem1.url)
      .flatMap(_.removeFetchItem(existingFetchItem2.url))
      .flatMap(_.removeFetchItem(existingFetchItem3.url))
      .flatMap(_.removeFetchItem(existingFetchItem4.url))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.fetchFiles shouldBe empty
    fetchTxt shouldNot exist

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
    sha1Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    sha1Manifest.contentAsString should not include bag.relativize(abc).toString
    sha256Manifest shouldNot exist

    // changes + save
    val newFile = testDir / "xxx.txt" createIfNotExists (createParents = true) writeText lipsum(5)
    bag.addPayloadManifestAlgorithm(ChecksumAlgorithm.SHA256)
      .flatMap(_.addPayloadFile(newFile)(_ / "abc.txt"))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    sha256Manifest should exist
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
    sha1Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)

    sha256Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA256)(u, v, w, x, y, z)

    // changes + save
    bag.removePayloadManifestAlgorithm(ChecksumAlgorithm.SHA256)
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    sha1Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)
    sha256Manifest shouldNot exist

    // validate bag
    BagVerifier.quicklyVerify(bag.locBag)
    BagLinter.lintBag(bag.path) should contain only(
      BagitWarning.DIFFERENT_CASE, // TODO https://github.com/LibraryOfCongress/bagit-java/issues/119
      BagitWarning.OLD_BAGIT_VERSION,
      BagitWarning.WEAK_CHECKSUM_ALGORITHM,
    )
  }

  it should "not remove empty manifest files" in {
    val bag = simpleBagV0()

    val u = bag.data / "sub" / "u"
    val v = bag.data / "sub" / "v"
    val w = bag.data / "sub" / "w"
    val x = bag.data / "x"
    val y = bag.data / "y"
    val z = bag.data / "z"

    val sha1Manifest = bag / "manifest-sha1.txt"

    // initial assumptions
    sha1Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)(u, v, w, x, y, z)

    // changes + save
    bag.removePayloadFile(bag.data.relativize(u))
      .flatMap(_.removePayloadFile(bag.data.relativize(v)))
      .flatMap(_.removePayloadFile(bag.data.relativize(w)))
      .flatMap(_.removePayloadFile(bag.data.relativize(x)))
      .flatMap(_.removePayloadFile(bag.data.relativize(y)))
      .flatMap(_.removePayloadFile(bag.data.relativize(z)))
      .flatMap(_.save()) shouldBe a[Success[_]]

    // expected results
    bag.data.list.toList shouldBe empty
    sha1Manifest should exist
    bag.baseDir should containInPayloadManifestFileOnly(ChecksumAlgorithm.SHA1)()

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
