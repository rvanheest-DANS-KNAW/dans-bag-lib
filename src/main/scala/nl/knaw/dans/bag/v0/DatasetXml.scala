package nl.knaw.dans.bag.v0

import better.files.File

import scala.util.Try

case class DatasetXml() {

  def save(file: File): Try[Unit] = ???
}

object DatasetXml {

  def empty(): DatasetXml = {
    DatasetXml()
  }

  def read(file: File): Try[DatasetXml] = ???
}
