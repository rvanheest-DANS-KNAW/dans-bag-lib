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
