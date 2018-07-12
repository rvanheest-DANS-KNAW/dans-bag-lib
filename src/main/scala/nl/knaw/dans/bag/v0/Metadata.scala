package nl.knaw.dans.bag.v0

import better.files.File
import nl.knaw.dans.bag.v0.Metadata.{ datasetXmlFilename, filesXmlFilename }

import scala.util.Try

case class Metadata private(datasetXml: DatasetXml,
                            filesXml: FilesXml) {

  def save(dir: File): Try[Unit] = {
    for {
      _ <- datasetXml.save(dir / datasetXmlFilename)
      _ <- filesXml.save(dir / filesXmlFilename)
    } yield ()
  }
}

object Metadata {

  val datasetXmlFilename = "dataset.xml"
  val filesXmlFilename = "files.xml"

  def empty(): Metadata = {
    Metadata(DatasetXml.empty(), FilesXml.empty())
  }

  def read(dir: File): Try[Metadata] = {
    for {
      datasetXml <- DatasetXml.read(dir / datasetXmlFilename)
      filesXml <- FilesXml.read(dir / filesXmlFilename)
    } yield Metadata(datasetXml, filesXml)
  }
}
