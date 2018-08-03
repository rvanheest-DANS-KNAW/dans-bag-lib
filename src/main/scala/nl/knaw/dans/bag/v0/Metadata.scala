package nl.knaw.dans.bag.v0


import better.files.File
import nl.knaw.dans.bag.v0.Metadata.filesXmlFilename
import scala.util.Try

case class Metadata private(filesXml: FilesXml) {

  def save(dir: File): Try[Unit] = {
    for {
      _ <- filesXml.save(dir / filesXmlFilename)
    } yield ()
  }
}

object Metadata {

  val filesXmlFilename = "files.xml"

  def empty(): Metadata = {
    Metadata(FilesXml.empty())
  }

  def read(dir: File): Try[Metadata] = {
    for {
      filesXml <- FilesXml.read(dir / filesXmlFilename)
    } yield Metadata(filesXml)
  }
}
