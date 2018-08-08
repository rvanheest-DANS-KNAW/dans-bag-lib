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
import nl.knaw.dans.bag.{ ChecksumAlgorithm, FetchItem, RelativePath, betterFileToPath }

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

  "addFetchItem with RelativePath" should "add the fetch item to the bag's list of fetch items" in {
    val fetchFileSrc = lipsum1URL
    assumeCanConnect(fetchFileSrc)

    val bag = fetchBagV0()

    inside(bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum1.txt")) {
      case Success(resultBag) =>
        resultBag.fetchFiles should contain(
          FetchItem(fetchFileSrc, lipsum1Size, bag.data / "to-be-fetched" / "lipsum1.txt")
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
    inside(bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum3.txt")) {
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

    inside(bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum4.txt")) {
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

    inside(bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum5.txt")) {
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

    bag.addFetchItem(fetchFileSrc, _ / "x") should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "x: already exists in payload").toString =>
    }
  }

  it should "fail when the destination already exists in the fetch.txt" in {
    val fetchFileSrc = lipsum1URL
    assumeCanConnect(fetchFileSrc)

    val bag = fetchBagV0()

    bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum1.txt")

    bag.addFetchItem(fetchFileSrc, _ / "to-be-fetched" / "lipsum1.txt") should matchPattern {
      case Failure(e: FileAlreadyExistsException) if e.getMessage == (bag.data / "to-be-fetched/lipsum1.txt: already exists in fetch.txt").toString =>
    }
  }

  it should "fail when the destination is not inside the bag/data directory" in {
    val bag = simpleBagV0()
    val fetchFileSrc = lipsum5URL

    bag.addFetchItem(fetchFileSrc, _ / ".." / "lipsum1.txt") should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == s"a fetch file can only point to a location inside the bag/data directory; ${ bag / "lipsum1.txt" } is outside the data directory" =>
    }
  }

  it should "fail when the file cannot be downloaded from the provided url" in {
    val bag = simpleBagV0()

    inside(bag.addFetchItem(new URL("http://x"), _ / "to-be-fetched" / "failing-url.txt")) {
      // it's either one of these exceptions that is thrown
      case Failure(e: SocketTimeoutException) =>
        e should have message "connect timed out"
      case Failure(e: UnknownHostException) =>
        e should have message "x"
    }
  }

  "addFetchItem with java.nio.file.Path" should "forward to the overload with RelativePath" in {
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

  "removeFetchItem with RelativePath" should "remove the fetch item from the list" in {
    val bag = fetchBagV0()
    val relativePath: RelativePath = _ / "x"
    val absolutePath = relativePath(bag.data)

    bag.fetchFiles.map(_.file) should contain(absolutePath)

    inside(bag.removeFetchItem(relativePath)) {
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

    val relativePath1: RelativePath = _ / "x"
    val relativePath2: RelativePath = _ / "y-old"
    val relativePath3: RelativePath = _ / "sub" / "u"
    val relativePath4: RelativePath = _ / "sub" / "v"

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
    val relativePath: RelativePath = _ / "not-existing-fetch-file"
    val absolutePath = relativePath(bag.data)

    bag.fetchFiles.map(_.file) should not contain absolutePath

    inside(bag.removeFetchItem(relativePath)) {
      case Failure(e: NoSuchFileException) =>
        e should have message absolutePath.toString
        bag.fetchFiles.map(_.file) should not contain absolutePath
    }
  }

  "removeFetchItem with java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = fetchBagV0()
    val relativePath: RelativePath = _ / "x"
    val absolutePath = relativePath(bag.data)

    bag.fetchFiles.map(_.file) should contain(absolutePath)

    inside(bag.removeFetchItem(Paths.get("x"))) {
      case Success(resultBag) =>
        resultBag.fetchFiles.map(_.file) should not contain absolutePath
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
    val url = new URL("http://not.existing/fetch/file")

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
    val fetchItem = FetchItem(new URL("http://not.existing/fetch/file"), 12L, bag.data / "not-existing-fetch-file")

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

  "replaceFileWithFetchItem with RelativePath" should "remove the file from the payload" in {
    val bag = fetchBagV0()

    (bag.data / "y").toJava should exist

    inside(bag.replaceFileWithFetchItem(_ / "y", new URL("http://y"))) {
      case Success(resultBag) =>
        (resultBag.data / "y").toJava shouldNot exist
    }
  }

  it should "remove any empty directories that are left behind after removing the file from the payload" in {
    val bag = fetchBagV0()

    (bag.data / "more" / "files" / "abc").toJava should exist

    inside(bag.replaceFileWithFetchItem(_ / "more" / "files" / "abc", new URL("http://abc"))) {
      case Success(resultBag) =>
        (resultBag.data / "more" / "files" / "abc").toJava shouldNot exist
        (resultBag.data / "more" / "files").toJava shouldNot exist
        (resultBag.data / "more").toJava shouldNot exist
        resultBag.data.toJava should exist
    }
  }

  it should "not remove the file from the payload manifests" in {
    val bag = fetchBagV0()

    forEvery(bag.payloadManifests) {
      case (_, manifest) =>
        manifest should contain key (bag.data / "y")
    }

    inside(bag.replaceFileWithFetchItem(_ / "y", new URL("http://y"))) {
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

    bag.fetchFiles.map(_.file) shouldNot contain(bag.data / "y")

    inside(bag.replaceFileWithFetchItem(_ / "y", new URL("http://y"))) {
      case Success(resultBag) =>
        resultBag.fetchFiles should contain(FetchItem(new URL("http://y"), yBytes, resultBag.data / "y"))
    }
  }

  it should "fail when the file does not exist in the payload manifest" in {
    val bag = fetchBagV0()

    (bag.data / "no-such-file.txt").toJava shouldNot exist

    inside(bag.replaceFileWithFetchItem(_ / "no-such-file.txt", new URL("http://xxx"))) {
      case Failure(e: NoSuchFileException) =>
        e should have message (bag.data / "no-such-file.txt").toString()
    }
  }

  it should "fail when the file is not inside the bag/data directory" in {
    val bag = fetchBagV0()

    inside(bag.replaceFileWithFetchItem(_ / ".." / "fetch.txt", new URL("http://xxx"))) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"a fetch file can only point to a location inside the bag/data directory; ${ bag / "fetch.txt" } is outside the data directory"
    }
  }

  it should "fail when the url another protocol than 'http' or 'https'" in {
    val bag = fetchBagV0()

    inside(bag.replaceFileWithFetchItem(_ / "y", (testDir / "y-new-location").url)) {
      case Failure(e: IllegalArgumentException) =>
        e should have message "url can only have protocol 'http' or 'https'"
    }
  }

  "replaceFileWithFetchItem with java.nio.file.Path" should "forward to the overload with RelativePath" in {
    val bag = fetchBagV0()

    (bag.data / "y").toJava should exist

    inside(bag.replaceFileWithFetchItem(Paths.get("y"), new URL("http://y"))) {
      case Success(resultBag) =>
        (resultBag.data / "y").toJava shouldNot exist
    }
  }

  "replaceFetchItemWithFile with RelativePath" should "resolve a fetch item by file" in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    x.toJava shouldNot exist

    inside(bag.replaceFetchItemWithFile(_ / "x")) {
      case Success(_) =>
        x.toJava should exist
    }
  }

  it should "fail when the file to be resolved does not occur in the list of fetch files" in {
    val bag = fetchBagV0()

    inside(bag.replaceFetchItemWithFile(_ / "non-existing-file")) {
      case Failure(e: IllegalArgumentException) =>
        e should have message s"path ${ bag.data / "non-existing-file" } does not occur in the list of fetch files"
    }
  }

  "replaceFetchItemWithFile with java.nio.file.Path" should "forward to the overload with RelativePath" in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    x.toJava shouldNot exist

    inside(bag.replaceFetchItemWithFile(Paths.get("x"))) {
      case Success(_) =>
        x.toJava should exist
    }
  }

  "replaceFetchItemWithFile with URL" should "resolve a fetch item by url" in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val url = lipsum4URL
    val path = lipsum4Dest(bag.data)

    path.toJava shouldNot exist

    inside(bag.replaceFetchItemWithFile(url)) {
      case Success(_) =>
        path.toJava should exist
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

  "replaceFetchItemWithFile with FetchItem" should "download the file and put it in the payload" in {
    assumeCanConnect(lipsum4URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"

    x.toJava shouldNot exist

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum4URL, 12L, x))) {
      case Success(_) =>
        x.toJava should exist
    }
  }

  it should "create the subdirectories that are necessary for placing the file in its proper place" in {
    assumeCanConnect(lipsum1URL)

    val bag = fetchBagV0()
    val subU = bag.data / "sub" / "u"

    subU.toJava shouldNot exist
    subU.parent.toJava shouldNot exist
    subU.parent.parent.toJava should exist

    inside(bag.replaceFetchItemWithFile(FetchItem(lipsum1URL, 12L, subU))) {
      case Success(_) =>
        subU.toJava should exist
    }
  }

  it should "remove the file from the list of fetch files" in {
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

    u.toJava should exist

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

  it should "fail when the URL is not resolvable" in {
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL, lipsum5URL)

    val bag = fetchBagV0()
    val yNew = bag.data / "y-new-file"
    val fetchItem = FetchItem(new URL("http://y-new-url"), 12L, yNew)
    bag.locBag.getItemsToFetch.add(fetchItem)

    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      // it's either one of these exceptions that is thrown
      case Failure(e: SocketTimeoutException) =>
        e should have message "connect timed out"
      case Failure(e: UnknownHostException) =>
        e should have message "y-new-url"
    }
  }

  it should "fail when the checksum of the downloaded file does not match the one listed in the payload manifests" in {
    assumeCanConnect(lipsum1URL, lipsum2URL, lipsum3URL, lipsum4URL, lipsum5URL)

    val bag = fetchBagV0()
    val x = bag.data / "x"
    val checksum = bag.payloadManifests(ChecksumAlgorithm.SHA1)(x)
    bag.locBag.getPayLoadManifests.asScala.headOption.value
      .getFileToChecksumMap
      .put(x, "invalid-checksum")

    x.toJava shouldNot exist

    val fetchItem = FetchItem(lipsum4URL, 12L, x)
    inside(bag.replaceFetchItemWithFile(fetchItem)) {
      case Failure(e: InvalidChecksumException) =>
        e should have message s"checksum (${ ChecksumAlgorithm.SHA1 }) of the downloaded file was 'invalid-checksum' but should be '$checksum'"

        bag.list.withFilter(_.isDirectory).map(_.name).toList should contain only(
          "data",
          "metadata",
        )
        bag.fetchFiles should contain(fetchItem)
        x.toJava shouldNot exist
    }
  }
}
