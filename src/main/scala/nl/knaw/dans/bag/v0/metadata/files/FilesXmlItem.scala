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

import better.files.File
import nl.knaw.dans.bag.v0.metadata.MetadataElement
import nl.knaw.dans.bag.v0.metadata.MetadataElementType.MetadataElementType
import nl.knaw.dans.bag.v0.metadata.files.FileAccessCategory.FileAccessCategory

import scala.util.Try

// TODO use Path API instead, since these are relative paths
case class FilesXmlItem(filepath: File,
                        mimetype: MimeType,
                        accessibleToRights: Option[FileAccessCategory] = Option.empty,
                        visibleToRights: Option[FileAccessCategory] = Option.empty,
                        metadataElements: Seq[MetadataElement] = Seq.empty) {

  def move(dest: File): FilesXmlItem = {
    this.copy(filepath = dest)
  }

  def updateMimetype(): Try[FilesXmlItem] = {
    MimeType.get(filepath)
      .map(newMimetype => this.copy(mimetype = newMimetype))
  }

  def withAccessibleToRights(access: FileAccessCategory): FilesXmlItem = {
    this.copy(accessibleToRights = Option(access))
  }

  def withoutAccessibleToRights: FilesXmlItem = {
    this.copy(accessibleToRights = Option.empty)
  }

  def withVisibleToRights(visible: FileAccessCategory): FilesXmlItem = {
    this.copy(visibleToRights = Option(visible))
  }

  def withoutVisibleToRights: FilesXmlItem = {
    this.copy(visibleToRights = Option.empty)
  }

  def getMetadataElement(metadataElementType: MetadataElementType): Seq[MetadataElement] = {
    metadataElements.filter(_.tag == metadataElementType)
  }

  def updateMetadataElements(f: Seq[MetadataElement] => Seq[MetadataElement]): FilesXmlItem = {
    this.copy(metadataElements = f(metadataElements))
  }
}
