package nl.knaw.dans.bag

import java.net.URL

import better.files.File
import gov.loc.repository.bagit.domain.{ FetchItem => LocFetchItem }

import scala.language.implicitConversions

case class FetchItem(url: URL, length: Long, file: File)

object FetchItem {
  implicit def locConverter(locFetchItem: LocFetchItem): FetchItem = {
    FetchItem(locFetchItem.getUrl, locFetchItem.getLength.toLong, locFetchItem.getPath)
  }

  implicit def locDeconverter(fetchItem: FetchItem): LocFetchItem = {
    new LocFetchItem(fetchItem.url, fetchItem.length, fetchItem.file.path)
  }
}
