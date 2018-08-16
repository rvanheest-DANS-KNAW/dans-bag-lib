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

import nl.knaw.dans.bag.fixtures._
import nl.knaw.dans.bag.{ ChecksumAlgorithm, betterFileToPath }
import org.scalatest.tagobjects.Retryable

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success }

class ManifestAlgorithmSpec extends TestSupportFixture
  with TestBags
  with BagMatchers
  with FetchFileMetadata
  with CanConnect {

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

  it should "calculate and add the checksums of fetch files to the newly added manifest" taggedAs Retryable in {
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
}
