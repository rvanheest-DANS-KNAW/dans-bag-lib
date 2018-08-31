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

import java.io.InputStream
import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException, Path }

import better.files.File
import nl.knaw.dans.bag.MimeType
import nl.knaw.dans.bag.v0.metadata.MetadataElement
import nl.knaw.dans.bag.FileAccessCategory.FileAccessCategory

import scala.collection.mutable
import scala.util.{ Failure, Try }

case class FilesXml private(private val files: mutable.Map[Path, FilesXmlItem] = mutable.Map.empty) {

  def list: Map[Path, FilesXmlItem] = files.toMap

  def getFiles: Iterable[Path] = files.keys

  def getItems: Iterable[FilesXmlItem] = files.values

  def get(file: Path): Option[FilesXmlItem] = files.get(file)

  def getInDirectory(dir: Path): Iterable[FilesXmlItem] = {
    files.filterKeys(_ startsWith dir).values
  }

  def add(item: FilesXmlItem): Try[FilesXml] = Try {
    this.get(item.filepath)
      .map(_ => throw new FileAlreadyExistsException(item.filepath.toString))
      .getOrElse { FilesXml(files += (item.filepath -> item)) }
  }

  def remove(file: Path): Try[FilesXml] = Try {
    this.get(file)
      .map(_ => FilesXml(files -= file))
      .getOrElse { throw new NoSuchFileException(file.toString) }
  }

  def update(file: Path, f: FilesXmlItem => FilesXmlItem): Try[FilesXml] = {
    this.get(file)
      .map(item => {
        for {
          intermediate <- this.remove(file)
          newItem = f(item)
          result <- intermediate.add(newItem)
        } yield result
      })
      .getOrElse(Failure(new NoSuchElementException(s"could not find file $file")))
  }

  def move(file: Path, dest: Path): Try[FilesXml] = {
    this.update(file, _ moveTo dest)
  }

  def updateMimetype(file: Path, mimetype: MimeType): Try[FilesXml] = {
    this.update(file, _ updateMimetype mimetype)
  }

  def withAccessibleToRights(file: Path, access: FileAccessCategory): Try[FilesXml] = {
    this.update(file, _ withAccessibleToRights access)
  }

  def withoutAccessibleToRights(file: Path): Try[FilesXml] = {
    this.update(file, _.withoutAccessibleToRights)
  }

  def withVisibleToRights(file: Path, access: FileAccessCategory): Try[FilesXml] = {
    this.update(file, _ withVisibleToRights access)
  }

  def withoutVisibleToRights(file: Path): Try[FilesXml] = {
    this.update(file, _.withoutVisibleToRights)
  }

//  def updateMetadataElements(file: Path,
//                             f: Seq[MetadataElement] => Seq[MetadataElement]): Try[FilesXml] = {
//    this.update(file, _ updateMetadataElements f)
//  }

  def save(file: File): Try[Unit] = {
    // TODO implemented in https://github.com/DANS-KNAW/dans-bag-lib/issues/15
    ???
  }
}

object FilesXml {

  def empty(): FilesXml = FilesXml()

  def read(file: File): Try[FilesXml] = file.inputStream()(FilesXml.read)

  def read(inputStream: InputStream): Try[FilesXml] = {
    // TODO implemented in https://github.com/DANS-KNAW/dans-bag-lib/issues/15
    ???
  }
}
