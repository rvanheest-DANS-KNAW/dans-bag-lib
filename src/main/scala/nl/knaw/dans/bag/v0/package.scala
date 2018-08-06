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

import nl.knaw.dans.bag.ChecksumAlgorithm.ChecksumAlgorithm

package object v0 {

  case class InvalidChecksumException private(private val msg: String) extends Exception(msg)

  object InvalidChecksumException {
    def apply(algorithm: ChecksumAlgorithm, actualChecksum: String,
              expectedChecksum: String): InvalidChecksumException = {
      new InvalidChecksumException(s"checksum ($algorithm) of the downloaded file was '$actualChecksum' but should be '$expectedChecksum'")
    }

    def apply(data: Seq[(ChecksumAlgorithm, String, String)]): InvalidChecksumException = {
      new InvalidChecksumException(s"checksums of downloaded file did match the actual checksums:\n" +
        data.map { case (algorithm, actualChecksum, expectedChecksum) =>
            s" - $algorithm: expected '$expectedChecksum', but was '$actualChecksum'"
        }.mkString("\n"))
    }
  }
}
