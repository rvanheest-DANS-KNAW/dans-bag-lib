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
