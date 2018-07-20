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
package nl.knaw.dans.bag.fixtures

import java.nio.charset.StandardCharsets

import better.files.File
import gov.loc.repository.bagit.domain.{ FetchItem => LocFetchItem }
import gov.loc.repository.bagit.hash.StandardBagitAlgorithmNameToSupportedAlgorithmMapping
import gov.loc.repository.bagit.reader.{ FetchReader, ManifestReader, MetadataReader }
import nl.knaw.dans.bag.ChecksumAlgorithm._
import nl.knaw.dans.bag.FetchItem
import nl.knaw.dans.bag.betterFileToPath
import org.scalatest.FlatSpecLike
import org.scalatest.matchers.{ MatchResult, Matcher }

import scala.collection.JavaConverters._
import scala.language.postfixOps

trait BagMatchers {
  this: FlatSpecLike =>

  type BagInfoEntry = (String, Seq[String])
  class ContainInBagInfoOnly(bagInfos: Seq[BagInfoEntry]) extends Matcher[File] {
    override def apply(baseDir: File): MatchResult = {
      val bagInfoFile = baseDir / "bag-info.txt"
      if (bagInfoFile.notExists) fail(s"$bagInfoFile does not exist")

      val bagInfoContent = MetadataReader.readBagMetadata(baseDir, StandardCharsets.UTF_8)
        .asScala.groupBy(_.getKey)
        .map { case (k, tuple) => k -> tuple.map(_.getValue) }

      MatchResult(
        bagInfoContent.size == bagInfos.size
          && bagInfos.forall { case (key, values) => bagInfoContent.get(key).exists(values ==) },
        s"$bagInfoContent did not contain only ${ bagInfos.mkString("(", ", ", ")") }",
        s"$bagInfoContent contained only ${ bagInfos.mkString("(", ", ", ")") }"
      )
    }
  }
  def containInBagInfoOnly(bagInfoHead: BagInfoEntry,
                           bagInfosTail: BagInfoEntry*): ContainInBagInfoOnly = {
    new ContainInBagInfoOnly(bagInfoHead :: bagInfosTail.toList)
  }

  class ContainInFetchOnly(fetchs: Seq[FetchItem]) extends Matcher[File] {
    override def apply(baseDir: File): MatchResult = {
      val fetchFile = baseDir / "fetch.txt"
      if (fetchFile.notExists) fail(s"$fetchFile does not exist")

      val fetchContent = FetchReader.readFetch(fetchFile, StandardCharsets.UTF_8, baseDir).asScala

      MatchResult(
        fetchContent.size == fetchs.size
          && fetchs.forall(fetch => fetchContent.contains(fetch: LocFetchItem)),
        s"$fetchContent did not contain only ${ fetchs.mkString("(", ", ", ")") }",
        s"$fetchContent contained only ${ fetchs.mkString("(", ", ", ")") }"
      )
    }
  }
  def containInFetchOnly(fetchHead: FetchItem, fetchTail: FetchItem*): ContainInFetchOnly = {
    new ContainInFetchOnly(fetchHead :: fetchTail.toList)
  }

  class ContainInManifestFileOnly(name: String, algorithm: ChecksumAlgorithm,
                                  files: Seq[File]) extends Matcher[File] {
    override def apply(baseDir: File): MatchResult = {
      val manifestFilename = s"$name-${ algorithm.getBagitName }.txt"
      val manifestFile = baseDir / manifestFilename
      if (manifestFile.notExists) fail(s"$manifestFile does not exist")

      val manifest = ManifestReader.readManifest(
        new StandardBagitAlgorithmNameToSupportedAlgorithmMapping,
        manifestFile, baseDir, StandardCharsets.UTF_8
      ).getFileToChecksumMap.asScala.map { case (path, checksum) => (File(path), checksum) }.toMap

      new ContainInManifestOnly(algorithm, files)(Map(algorithm -> manifest))
    }
  }

  def containInPayloadManifestFileOnly(algorithm: ChecksumAlgorithm)
                                      (files: File*): ContainInManifestFileOnly = {
    new ContainInManifestFileOnly("manifest", algorithm, files)
  }

  def containInTagManifestFileOnly(algorithm: ChecksumAlgorithm)
                                  (files: File*): ContainInManifestFileOnly = {
    new ContainInManifestFileOnly("tagmanifest", algorithm, files)
  }

  class ContainInManifestOnly(algorithm: ChecksumAlgorithm,
                              files: Seq[File]) extends Matcher[Map[ChecksumAlgorithm, Map[File, String]]] {
    def apply(manifests: Map[ChecksumAlgorithm, Map[File, String]]): MatchResult = {
      val manifest = manifests(algorithm)

      MatchResult(
        manifest.size == files.size &&
          manifest.forall { case (path, checksum) =>
            val pathFile = File(path)
            files.contains(pathFile) && pathFile.checksum(algorithm).toLowerCase == checksum
          },
        s"$manifest did not contain only ${ files.mkString("(", ", ", ")") }",
        s"$manifest contained only ${ files.mkString("(", ", ", ")") }"
      )
    }
  }

  def containInManifestOnly(algorithm: ChecksumAlgorithm)(files: File*): ContainInManifestOnly = {
    new ContainInManifestOnly(algorithm, files)
  }
}
