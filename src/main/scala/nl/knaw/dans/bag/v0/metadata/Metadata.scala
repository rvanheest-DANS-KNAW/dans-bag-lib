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
package nl.knaw.dans.bag.v0.metadata

import better.files.File
import nl.knaw.dans.bag.v0.metadata.Metadata.filesXmlFilename
import nl.knaw.dans.bag.v0.metadata.files.FilesXml

import scala.util.Try

/*
 * problem: on the one hand, we don't want to make users of the API write nested code like below,
 * but on the other hand, we also don't want to duplicate the methods of FilesXmlItem into Dans(V0)Bag
 * which would imply tight coupling.
 *
 * this is not really what we want:
 *   bag.updateMetadata(oldMetadata => oldMetadata
 *          .updateFilesXml(oldFilesXml => oldFilesXml
 *              .update(file, oldFilesXmlItem => oldFilesXmlItem
 *                  .withAccessibleToRights(FileAccessCategory.RESTRICTED_GROUP))))
 *
 * but this would cause coupling:
 *   bag.fileWithAccessibleToRight(file, FileAccessCategory.RESTRICTED_GROUP)
 *
 * a solution would be to have a FileUpdater with static methods
 *   FileUpdater.updateFileAccessibleToRight(bag, file, FileAccessCategory.RESTRICTED_GROUP)
 *
 * which could be rewritten using implicits:
 *   bag.updateFileAccessibleToRight(file, FileAccessCategory.RESTRICTED_GROUP)
 *
 * However, that might not really fit with the DansBag -> DansV0Bag model
 *
 *   // define implicit FileUpdater on DansBag, then you need to open up the access levels of the
 *   // DansV0Bag fields to be able to access them from outside the v0 package.
 *   implicit class FileUpdater(val bag: DansBag) extends AnyVal {
 *     def updateFileAccessibleToRight(file: File, fac: FileAccessCategory): DansBag = ???
 *   }
 *
 *   // define implicit FileUpdater on DansV0Bag, then you cannot use it from a DansBag instance
 *   implicit class FileUpdater(val bag: DansV0Bag) extends AnyVal {
 *     def updateFileAccessibleToRight(file: File, fac: FileAccessCategory): DansBag = ???
 *   }
 */

class Metadata private(filesXml: FilesXml) {

  def getFilesXml: FilesXml = filesXml

  def updateFilesXml(f: FilesXml => FilesXml): Metadata = {
    new Metadata(f(filesXml))
  }

  def save(dir: File): Try[Unit] = {
    for {
      _ <- filesXml.save(dir / filesXmlFilename)
    } yield ()
  }
}

object Metadata {

  val filesXmlFilename = "files.xml"

  def empty(): Metadata = {
    new Metadata(FilesXml.empty())
  }

  def read(dir: File): Try[Metadata] = {
    for {
      filesXml <- FilesXml.read(dir / filesXmlFilename)
    } yield new Metadata(filesXml)
  }
}
