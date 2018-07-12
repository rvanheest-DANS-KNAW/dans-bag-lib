package nl.knaw.dans.bag.v0

import better.files.File

import scala.util.Try

case class FilesXml() {

  def save(file: File): Try[Unit] = ???
}

object FilesXml {

  def empty(): FilesXml = {
    FilesXml()
  }

  def read(file: File): Try[FilesXml] = ???
}
