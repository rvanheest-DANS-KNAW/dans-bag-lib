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
package nl.knaw.dans.bag.v0.metadata.dataset

import nl.knaw.dans.bag.v0.metadata.files.FileAccessCategory
import nl.knaw.dans.bag.v0.metadata.files.FileAccessCategory.FileAccessCategory

object DatasetAccessCategory extends Enumeration {
  type DatasetAccessCategory = Value

  // @formatter:off
  val OPEN_ACCESS                      : DatasetAccessCategory = Value
  val OPEN_ACCESS_FOR_REGISTERED_USERS : DatasetAccessCategory = Value
  val GROUP_ACCESS                     : DatasetAccessCategory = Value
  val REQUEST_PERMISSION               : DatasetAccessCategory = Value
  val NO_ACCESS                        : DatasetAccessCategory = Value
  // @formatter:on

  implicit class DACValue(val dac: DatasetAccessCategory) extends AnyVal {
    def toFileAccessCategory: FileAccessCategory = {
      dac match {
        // @formatter:off
        case OPEN_ACCESS                      => FileAccessCategory.ANONYMOUS
        case OPEN_ACCESS_FOR_REGISTERED_USERS => FileAccessCategory.KNOWN
        case GROUP_ACCESS                     => FileAccessCategory.RESTRICTED_GROUP
        case REQUEST_PERMISSION               => FileAccessCategory.RESTRICTED_REQUEST
        case NO_ACCESS                        => FileAccessCategory.NONE
        // @formatter:on
      }
    }

    def toFileVisibleTo: FileAccessCategory = FileAccessCategory.ANONYMOUS
  }
}
