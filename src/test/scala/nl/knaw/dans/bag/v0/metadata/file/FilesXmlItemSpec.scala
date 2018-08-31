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
package nl.knaw.dans.bag.v0.metadata.file

import java.nio.file.Paths

import nl.knaw.dans.bag.FileAccessCategory
import nl.knaw.dans.bag.fixtures.{ TestBags, TestSupportFixture }
import nl.knaw.dans.bag.v0.metadata.files.FilesXmlItem
import nl.knaw.dans.bag.v0.metadata.{ MetadataElement, MetadataElementType, XmlLang }

class FilesXmlItemSpec extends TestSupportFixture with TestBags {

  private val file1: FilesXmlItem = {
    val file = Paths.get("data/x")
    val mimetype = "text/plain"
    val accessibleToRights = FileAccessCategory.RESTRICTED_REQUEST
    val visibleToRights = FileAccessCategory.ANONYMOUS
//    val metadataElements = Seq(
//      MetadataElement(
//        tag = MetadataElementType.DC_TITLE,
//        value = "my title"
//      ),
//      MetadataElement(
//        tag = MetadataElementType.DCTERMS_DESCRIPTION,
//        value = "my description",
//        language = Some(XmlLang.English)
//      )
//    )
    val elem =
      <file filepath={file.toString}>
        <dcterms:format>{mimetype}</dcterms:format>
        <accessibleToRights>{accessibleToRights.toString}</accessibleToRights>
        <visibleToRights>{visibleToRights.toString}</visibleToRights>
        <dc:title>my title</dc:title>
        <dcterms:description xml:lang="eng">my description</dcterms:description>
      </file>
    FilesXmlItem(file, mimetype, Some(accessibleToRights), Some(visibleToRights), elem)
  }

  private val file2: FilesXmlItem = {
    val file = Paths.get("data/y")
    val mimetype = "text/plain"
    val elem =
      <file filepath={file.toString}>
        <dcterms:format>{mimetype}</dcterms:format>
      </file>
    FilesXmlItem(file, mimetype, elem = elem)
  }

  "filepath" should "get the filepath from the FilesXmlItem" in {
    file1.filepath shouldBe Paths.get("data/x")
  }

  "mimetype" should "get the mimetype from the FilesXmlItem" in {
    file1.mimetype shouldBe "text/plain"
  }

  "accessibleToRights" should "get the accessibleToRights from the FilesXmlItem" in {
    file1.accessibleToRights.value shouldBe FileAccessCategory.RESTRICTED_REQUEST
  }

  it should "get None when the accessibleToRights is not present" in {
    file2.accessibleToRights shouldBe empty
  }

  "visibleToRights" should "get the visibleToRights from the FilesXmlItem" in {
    file1.visibleToRights.value shouldBe FileAccessCategory.ANONYMOUS
  }

  it should "get None when the visibleToRights is not present" in {
    file2.visibleToRights shouldBe empty
  }

//  "metadataElements" should "get the metadataElements listing from the FilesXmlItem" in {
//    file1.metadataElements should contain only(
//      MetadataElement(
//        tag = MetadataElementType.DC_TITLE,
//        value = "my title"
//      ),
//      MetadataElement(
//        tag = MetadataElementType.DCTERMS_DESCRIPTION,
//        value = "my description",
//        language = Some(XmlLang.English)
//      )
//    )
//  }
//
//  it should "get an empty listing when no metadataElements were provided" in {
//    file2.metadataElements shouldBe empty
//  }
}
