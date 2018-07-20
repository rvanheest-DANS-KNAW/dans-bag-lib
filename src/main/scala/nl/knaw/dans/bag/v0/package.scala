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
