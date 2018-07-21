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

import java.security.MessageDigest

import gov.loc.repository.bagit.hash.{ StandardSupportedAlgorithms, SupportedAlgorithm }

import scala.language.implicitConversions

object ChecksumAlgorithm extends Enumeration {

  type ChecksumAlgorithm = Value

  val MD5: ChecksumAlgorithm = Value("MD5")
  val SHA1: ChecksumAlgorithm = Value("SHA-1")
  val SHA256: ChecksumAlgorithm = Value("SHA-256")
  val SHA512: ChecksumAlgorithm = Value("SHA-512")

  implicit def locConverter(alg: SupportedAlgorithm): ChecksumAlgorithm = {
    ChecksumAlgorithm.withName(alg.getMessageDigestName)
  }

  implicit def locDeconverter(alg: ChecksumAlgorithm): SupportedAlgorithm = {
    StandardSupportedAlgorithms.values()
      .find(ssa => ssa.getMessageDigestName == alg.toString)
      .getOrElse {
        StandardSupportedAlgorithms.valueOf(alg.toString)
      }
  }

  implicit def algorithmToMessageDigest(checksumAlgorithm: ChecksumAlgorithm): MessageDigest =
    MessageDigest.getInstance(checksumAlgorithm.toString)
}
