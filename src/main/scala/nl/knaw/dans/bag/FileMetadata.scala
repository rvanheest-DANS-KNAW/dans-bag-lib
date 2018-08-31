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
package nl.knaw.dans.bag

import java.nio.file.Path

import nl.knaw.dans.bag.FileAccessCategory.FileAccessCategory

trait FileMetadata {

  val filepath: Path
  val mimetype: MimeType
  val accessibleToRights: Option[FileAccessCategory] = Option.empty
  val visibleToRights: Option[FileAccessCategory] = Option.empty
  // TODO other fields such as the dc/dcterms elements

  def moveTo(dest: Path): FileMetadata

  def updateMimetype(mimetype: MimeType): FileMetadata

  def withAccessibleToRights(access: FileAccessCategory): FileMetadata

  def withoutAccessibleToRights: FileMetadata

  def withVisibleToRights(visible: FileAccessCategory): FileMetadata

  def withoutVisibleToRights: FileMetadata

  // TODO other methods for updating the dc/dcterms elements
}
