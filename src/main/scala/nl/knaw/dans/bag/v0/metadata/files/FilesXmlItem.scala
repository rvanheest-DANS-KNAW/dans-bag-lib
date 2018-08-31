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
package nl.knaw.dans.bag.v0.metadata.files

import java.nio.file.Path

import nl.knaw.dans.bag.FileAccessCategory.FileAccessCategory
import nl.knaw.dans.bag.{ FileMetadata, MimeType }
import nl.knaw.dans.bag.v0.metadata.MetadataElement
import nl.knaw.dans.bag.v0.metadata.MetadataElementType.MetadataElementType

import scala.xml.Node

case class FilesXmlItem(filepath: Path,
                        mimetype: MimeType,
                        override val accessibleToRights: Option[FileAccessCategory] = Option.empty,
                        override val visibleToRights: Option[FileAccessCategory] = Option.empty,
//                        metadataElements: Seq[MetadataElement] = Seq.empty
                        elem: Node) extends FileMetadata {

  override def moveTo(dest: Path): FilesXmlItem = {
    this.copy(filepath = dest)
  }

  override def updateMimetype(mimetype: MimeType): FilesXmlItem = {
    this.copy(mimetype = mimetype)
  }

  override def withAccessibleToRights(access: FileAccessCategory): FilesXmlItem = {
    this.copy(accessibleToRights = Option(access))
  }

  override def withoutAccessibleToRights: FilesXmlItem = {
    this.copy(accessibleToRights = Option.empty)
  }

  override def withVisibleToRights(visible: FileAccessCategory): FilesXmlItem = {
    this.copy(visibleToRights = Option(visible))
  }

  override def withoutVisibleToRights: FilesXmlItem = {
    this.copy(visibleToRights = Option.empty)
  }

//  def getMetadataElement(metadataElementType: MetadataElementType): Seq[MetadataElement] = {
//    metadataElements.filter(_.tag == metadataElementType)
//  }
//
//  def updateMetadataElements(f: Seq[MetadataElement] => Seq[MetadataElement]): FilesXmlItem = {
//    this.copy(metadataElements = f(metadataElements))
//  }
}
