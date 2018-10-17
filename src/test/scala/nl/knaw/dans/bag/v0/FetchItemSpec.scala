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

import java.net.{ SocketTimeoutException, URL, UnknownHostException }
import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException, Paths }

import nl.knaw.dans.bag.fixtures._
import nl.knaw.dans.bag.{ ChecksumAlgorithm, FetchItem, betterFileToPath }
import org.scalatest.tagobjects.Retryable

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success }

class FetchItemSpec extends TestSupportFixture
  with TestBags
  with BagMatchers
  with Lipsum
  with FetchFileMetadata
  with CanConnect {

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

  "addFetchItem" should "add the fetch item to the bag's list of fetch items" taggedAs Retryable in {
    val fetchFileSrc = lipsum1URL
    assumeCanConnect(fetchFileSrc)

    val bag = fetchBagV0()

    inside(bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum1.txt"))) {
      case Success(resultBag) =>
        resultBag.fetchFiles should contain(
          FetchItem(fetchFileSrc, lipsum1Size, bag.data / "to-be-fetched" / "lipsum1.txt")
        )
    }
  }

  it should "download the file and calculate all checksums for the file" taggedAs Retryable in {
    val fetchFileSrc = lipsum2URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()

    bag.payloadManifestAlgorithms should contain only(
      ChecksumAlgorithm.SHA1,
      ChecksumAlgorithm.SHA256,
    )
    inside(bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum3.txt"))) {
      case Success(resultBag) =>
        val destination = resultBag.data / "to-be-fetched" / "lipsum3.txt"
        resultBag.payloadManifests(ChecksumAlgorithm.SHA1) should contain(destination -> lipsum2Sha1)
        resultBag.payloadManifests(ChecksumAlgorithm.SHA256) should contain(destination -> lipsum2Sha256)
    }
  }

  it should "remove the file after having calculated the checksums" taggedAs Retryable in {
    val fetchFileSrc = lipsum3URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()

    val beforeListing = bag.listRecursively.toList

    inside(bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum4.txt"))) {
      case Success(resultBag) =>
        val afterListing = resultBag.listRecursively.toList

        // While the file was downloaded to the root of the bag, it is removed again once the
        // checksums are calculated.
        afterListing should contain theSameElementsInOrderAs beforeListing
    }
  }

  it should "add the fetch.txt file to all tag manifests when a first fetch.txt file was added" taggedAs Retryable in {
    val fetchFileSrc = lipsum4URL
    assumeCanConnect(fetchFileSrc)

    val bag = multipleManifestsBagV0()
    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should not contain key(bag.baseDir / "fetch.txt")
    }

    inside(bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum5.txt"))) {
      case Success(resultBag) =>
        forEvery(resultBag.tagManifests) {
          case (_, manifest) =>
            manifest should contain key (resultBag.baseDir / "fetch.txt")
        }
    }
  }

  it should "fail when the destination already exists in the payload" in {
    val bag = simpleBagV0()
    val fetchFileSrc = lipsum4URL

    bag.addFetchItem(fetchFileSrc, Paths.get("x")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "x: already exists in payload").toString =>
    }
  }

  it should "fail when the destination already exists in the fetch.txt" taggedAs Retryable in {
    val fetchFileSrc = lipsum1URL
    assumeCanConnect(fetchFileSrc)

    val bag = fetchBagV0()

    bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum1.txt"))

    bag.addFetchItem(fetchFileSrc, Paths.get("to-be-fetched/lipsum1.txt")) should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "to-be-fetched/lipsum1.txt: already exists in fetch.txt").toString =>
    }
  }

  it should "fail when the destination is not inside the bag/data directory" in {
    val bag = simpleBagV0()
    val fetchFileSrc = lipsum5URL

    bag.addFetchItem(fetchFileSrc, Paths.get("../lipsum1.txt")) should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == s"a fetch file can only point to a location inside the bag/data directory; ${ bag / "lipsum1.txt" } is outside the data directory" =>
    }
  }

  it should "fail when the file cannot be downloaded from the provided url" in {
    val bag = simpleBagV0()

    inside(bag.addFetchItem(new URL("http://not.existing.dans.knaw.nl"), Paths.get("to-be-fetched/failing-url.txt"))) {
      // it's either one of these exceptions that is thrown
      case Failure(e: SocketTimeoutException) =>
        e should have message "connect timed out"
      case Failure(e: UnknownHostException) =>
        e should have message "not.existing.dans.knaw.nl"
    }
  }

  "removeFetchItem" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val relativePath = Paths.get("x")
    val absolutePath = bag.data / relativePath.toString

    bag.fetchFiles.map(_.file) should contain(absolutePath)

    inside(bag.removeFetchItem(relativePath)) {
      case Success(resultBag) =>
        resultBag.fetchFiles.map(_.file) should not contain absolutePath
    }
  }

  it should "remove the fetch item from all payload manifests" in {
    val bag = fetchBagV0()
    val relativePath = Paths.get("x")
    val absolutePath = bag.data / relativePath.toString

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key absolutePath
    }

    inside(bag.removeFetchItem(relativePath)) {
      case Success(resultBag) =>
        forEvery(resultBag.payloadManifests) {
          case (_, manifest) =>
            manifest shouldNot contain key absolutePath
        }
    }
  }

  it should "remove fetch.txt from all tag manifests when the last fetch file was removed" in {
    val bag = fetchBagV0()

    val relativePath1 = Paths.get("x")
    val relativePath2 = Paths.get("y-old")
    val relativePath3 = Paths.get("sub/u")
    val relativePath4 = Paths.get("sub/v")

    forEvery(bag.tagManifests) {
      case (_, manifest) =>
        manifest should contain key bag.baseDir / "fetch.txt"
    }

    val result = bag.removeFetchItem(relativePath1)
      .flatMap(_.removeFetchItem(relativePath2))
      .flatMap(_.removeFetchItem(relativePath3))
      .flatMap(_.removeFetchItem(relativePath4))

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
    val relativePath = Paths.get("not-existing-fetch-file")
    val absolutePath = bag.data / relativePath.toString

    bag.fetchFiles.map(_.file) should not contain absolutePath

    inside(bag.removeFetchItem(relativePath)) {
      case Failure(e: NoSuchFileException) =>
        e should have message absolutePath.toString
        bag.fetchFiles.map(_.file) should not contain absolutePath
    }
  }

  "removeFetchItem with URL" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val url = lipsum1URL

    bag.fetchFiles.map(_.url) should contain(url)

    inside(bag.removeFetchItem(url)) {
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

        inside(bag.removeFetchItem(url)) {
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

    val result = bag.removeFetchItem(url1)
      .flatMap(_.removeFetchItem(url2))
      .flatMap(_.removeFetchItem(url3))
      .flatMap(_.removeFetchItem(url4))

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
    val url = new URL("http://not.existing.dans.knaw.nl/fetch/file")

    bag.fetchFiles.map(_.url) should not contain url

    inside(bag.removeFetchItem(url)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"no such URL: $url"
        bag.fetchFiles.map(_.url) should not contain url
    }
  }

  "removeFetchItem with FetchItem" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val fetchItem = bag.fetchFiles.head

    val resultBag = bag.removeFetchItem(fetchItem)
    resultBag.fetchFiles should not contain fetchItem
  }

  it should "remove the fetch item from all payload manifests" in {
    val bag = fetchBagV0()
    val fetchItem = bag.fetchFiles.head

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key fetchItem.file
    }

    val resultBag = bag.removeFetchItem(fetchItem)

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

    val resultBag = bag.removeFetchItem(fetch1)
      .removeFetchItem(fetch2)
      .removeFetchItem(fetch3)
      .removeFetchItem(fetch4)

    forEvery(resultBag.tagManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key bag.baseDir / "fetch.txt"
    }
  }

  it should "not fail when the fetch item is not a part of the bag" in {
    val bag = fetchBagV0()
    val fetchItem = FetchItem(new URL("http://not.existing.dans.knaw.nl/fetch/file"), 12L, bag.data / "not-existing-fetch-file")

    bag.fetchFiles should not contain fetchItem
    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key fetchItem.file
    }

    val resultBag = bag.removeFetchItem(fetchItem)

    resultBag.fetchFiles should not contain fetchItem
    forEvery(resultBag.payloadManifests) {
      case (_, manifest) =>
        manifest shouldNot contain key fetchItem.file
    }
  }

  "replaceFileWithFetchItem" should "remove the file from the payload" in {
    val bag = fetchBagV0()

    (bag.data / "y") should exist

    inside(bag.replaceFileWithFetchItem(Paths.get("y"), new URL("http://not.existing.dans.knaw.nl/y"))) {
      case Success(resultBag) =>
        (resultBag.data / "y") shouldNot exist
    }
  }

  it should "remove any empty directories that are left behind after removing the file from the payload" in {
    val bag = fetchBagV0()

    (bag.data / "more" / "files" / "abc") should exist

    inside(bag.replaceFileWithFetchItem(Paths.get("more/files/abc"), new URL("http://not.existing.dans.knaw.nl/abc"))) {
      case Success(resultBag) =>
        (resultBag.data / "more" / "files" / "abc") shouldNot exist
        (resultBag.data / "more" / "files") shouldNot exist
        (resultBag.data / "more") shouldNot exist
        resultBag.data should exist
    }
  }

  it should "not remove the file from the payload manifests" in {
    val bag = fetchBagV0()

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key (bag.data / "y")
    }

    inside(bag.replaceFileWithFetchItem(Paths.get("y"), new URL("http://not.existing.dans.knaw.nl/y"))) {
      case Success(resultBag) =>
        forEvery(resultBag.payloadManifests) {
          case (_, manifest) =>
            manifest should contain key (resultBag.data / "y")
        }
    }
  }

  it should "add the file to the list of fetch files" in {
    val bag = fetchBagV0()
    val yBytes = (bag.data / "y").size
    val url = new URL("http://not.existing.dans.knaw.nl/y")

    bag.fetchFiles.map(_.file) shouldNot contain(bag.data / "y")

    inside(bag.replaceFileWithFetchItem(Paths.get("y"), url)) {
      case Success(resultBag) =>
        resultBag.fetchFiles should contain(FetchItem(url, yBytes, resultBag.data / "y"))
    }
  }

  it should "fail when the file does not exist in the payload manifest" in {
    val bag = fetchBagV0()

    (bag.data / "no-such-file.txt") shouldNot exist

    inside(bag.replaceFileWithFetchItem(Paths.get("no-such-file.txt"), new URL("http://not.existing.dans.knaw.nl/xxx"))) {
      case Failure(e: NoSuchFileException) =>
        e should have message (bag.data / "no-such-file.txt").toString()
    }
  }

  it should "fail when the file is not inside the bag/data directory" in {
    val bag = fetchBagV0()

    inside(bag.replaceFileWithFetchItem(Paths.get("../fetch.txt"), new URL("http://not.existing.dans.knaw.nl/xxx"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"a fetch file can only point to a location inside the bag/data directory; ${ bag / "fetch.txt" } is outside the data directory"
    }
  }

  it should "fail when the url another protocol than 'http' or 'https'" in {
    val bag = fetchBagV0()

    inside(bag.replaceFileWithFetchItem(Paths.get("y"), (testDir / "y-new-location").url)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "url can only have protocol 'http' or 'https'"
    }
  }

  "replaceFetchItemWithFile with Path" should "resolve a fetch item by file" taggedAs Retryable in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    x shouldNot exist

    inside(bag.replaceFetchItemWithFile(Paths.get("x"))) {
      case Success(_) =>
        x should exist
    }
  }

  it should "fail when the file to be resolved does not occur in the list of fetch files" in {
    val bag = fetchBagV0()

    inside(bag.replaceFetchItemWithFile(Paths.get("non-existing-file"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"path ${ bag.data / "non-existing-file" } does not occur in the list of fetch files"
    }
  }

  "replaceFetchItemWithFile with URL" should "resolve a fetch item by url" taggedAs Retryable in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val url = lipsum4URL
    val path = bag.data / lipsum4Dest.toString

    path shouldNot exist

    inside(bag.replaceFetchItemWithFile(url)) {
      case Success(_) =>
        path should exist
    }
  }

  it should "fail when the url does not have a 'http' or 'https' protocol" in {
    val bag = fetchBagV0()
    val file = testDir / "test-file.txt" writeText lipsum(2)

    inside(bag.replaceFetchItemWithFile(file.url)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "url can only have protocol 'http' or 'https'"
    }
  }

  it should "fail when the url to be resolved does not occur in the list of fetch files" in {
    val bag = fetchBagV0()

    inside(bag.replaceFetchItemWithFile(lipsum5URL)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"no such url: $lipsum5URL"
    }
  }

  "replaceFetchItemWithFile with FetchItem" should "download the file and put it in the payload" taggedAs Retryable in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    x shouldNot exist

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum4URL, 12L, x))) {
      case Success(_) =>
        x should exist
    }
  }

  it should "create the subdirectories that are necessary for placing the file in its proper place" taggedAs Retryable in {
    assumeCanConnect(lipsum1URL)

    val bag = fetchBagV0()
    val subU = bag.data / "sub" / "u"

    subU shouldNot exist
    subU.parent shouldNot exist
    subU.parent.parent should exist

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum1URL, 12L, subU))) {
      case Success(_) =>
        subU should exist
    }
  }

  it should "remove the file from the list of fetch files" taggedAs Retryable in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    bag.fetchFiles.map(_.file) should contain(x)

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum4URL, 12L, x))) {
      case Success(resultBag) =>
        resultBag.fetchFiles.map(_.file) should not contain x
    }
  }

  it should "fail when the file to be resolved already exists within the bag" in {
    val bag = fetchBagV0()
    val u = (bag.data / "sub" createDirectory()) / "u" writeText lipsum(1)

    u should exist

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum1URL, 12L, u))) {
      case Failure(e: FileAlreadyExistsException) =>
        e should have message u.toString
    }
  }

  it should "fail when the file to be resolved points outside of the be bag/data directory" in {
    val bag = fetchBagV0()
    val test = bag / "test"
    val fetchItem = FetchItem(lipsum4URL, 12L, test)

    bag.locBag.getItemsToFetch.add(fetchItem)

    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"a fetch file can only point to a location inside the bag/data directory; $test is outside the data directory"
    }
  }

  it should "fail when the file to be resolved does not occur in the list of fetch files" in {
    val bag = fetchBagV0()
    val test = bag.data / "non-existing-file"
    val fetchItem = FetchItem(lipsum4URL, 12L, test)

    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"fetch item $fetchItem does not occur in the list of fetch files"
    }
  }

  it should "fail when the URL is not resolvable" taggedAs Retryable in {
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL, lipsum5URL)

    val bag = fetchBagV0()
    val yNew = bag.data / "y-new-file"
    val fetchItem = FetchItem(new URL("http://not.existing.dans.knaw.nl/y-new-url"), 12L, yNew)
    bag.locBag.getItemsToFetch.add(fetchItem)

    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      // it's either one of these exceptions that is thrown
      case Failure(e: SocketTimeoutException) =>
        e should have message "connect timed out"
      case Failure(e: UnknownHostException) =>
        e should have message "not.existing.dans.knaw.nl"
    }
  }

  it should "fail when the checksum of the downloaded file does not match the one listed in the payload manifests" taggedAs Retryable in {
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL, lipsum5URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"
    val checksum = bag.payloadManifests(ChecksumAlgorithm.SHA1)(x)
    bag.locBag.getPayLoadManifests.asScala.headOption.value
      .getFileToChecksumMap
      .put(x, "invalid-checksum")

    x shouldNot exist

    val fetchItem = FetchItem(lipsum4URL, 12L, x)
    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      case Failure(e: InvalidChecksumException) =>
        e should have message s"checksum (${ ChecksumAlgorithm.SHA1 }) of the downloaded file was 'invalid-checksum' but should be '$checksum'"

        bag.list.withFilter(_.isDirectory).map(_.name).toList should contain only(
          "data",
          "metadata",
        )
        bag.fetchFiles should contain(fetchItem)
        x shouldNot exist
    }
  }
}
