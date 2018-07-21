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
