package nl.knaw.dans.bag.fixtures

import java.net.URL

trait FetchFileMetadata {
  this: TestSupportFixture =>

  val lipsum1URL: URL = new URL("https://raw.githubusercontent.com/rvanheest-DANS-KNAW/" +
    "dans-bag-lib/c8681a5932e4081dfec95680abefc9a07740a97a/src/test/resources/fetch-files/" +
    "lipsum1.txt")
  val lipsum1Md5: String = "82d227831f4a3dda60e0c7c3506fa6db"
  val lipsum1Sha1: String = "33f7f7bc4d15e7749a7bace2ff57431aec260491"
  val lipsum1Sha256: String = "0aecd2e3362b4a3f5ac2f7bd048bab000ad67ae7a0b1d5cf5969e2aba09dbf10"
  val lipsum1Sha512: String = "220ab5bd2008b9f8a03f2738925dc1367d51d26708571c590323d5f5aa2310d6" +
    "19d5cdbb6f0f84be9145643ac793dd63eaee407e51d6dacd5ba3c1d8f6fe07da"

  val lipsum2URL: URL = new URL("https://raw.githubusercontent.com/rvanheest-DANS-KNAW/" +
    "dans-bag-lib/c8681a5932e4081dfec95680abefc9a07740a97a/src/test/resources/fetch-files/" +
    "lipsum2.txt")
  val lipsum2Md5: String = "31b4c84b092718937e8a1c80d7c27564"
  val lipsum2Sha1: String = "7ae3025002cfd8eb40434dd7aacb60b94e367de1"
  val lipsum2Sha256: String = "79a5e42fb48304405d9c465c5b6bff3bd1115cb6e7cb34940eb16216791b2e01"
  val lipsum2Sha512: String = "a1fff27b172765ac4d6a0b461647f8f63f6c65e3833c9656886da5663721917d" +
    "d4086396119e2ebaadd79548d623d8dfa350f854af6271da68311abb1c0e73e8"

  val lipsum3URL: URL = new URL("https://raw.githubusercontent.com/rvanheest-DANS-KNAW/" +
    "dans-bag-lib/c8681a5932e4081dfec95680abefc9a07740a97a/src/test/resources/fetch-files/" +
    "lipsum3.txt")
  val lipsum3Md5: String = "ec90f91c350a6815e455ef24d4ccf2ae"
  val lipsum3Sha1: String = "a98762acfd57cadd6c5ce143fe94fec46ff36d0c"
  val lipsum3Sha256: String = "b12a3260c2837d62edb2209d6199b91b7653a4dc1200d8335066e333128f01cb"
  val lipsum3Sha512: String = "a55358721ff1bf6394427c73a67104c3e182f95062c4e7a1a367fb860c1b98ae" +
    "647c11784b75eed3a20ac60efda146f040daeaa3837fa3aba9a2b94e30da0965"

  val lipsum4URL: URL = new URL("https://raw.githubusercontent.com/rvanheest-DANS-KNAW/" +
    "dans-bag-lib/c8681a5932e4081dfec95680abefc9a07740a97a/src/test/resources/fetch-files/" +
    "lipsum4.txt")
  val lipsum4Md5: String = "bcbcedce7cf3849ca33bf2266ce1a39f"
  val lipsum4Sha1: String = "69ce0ab59d166a3ebccb0dca3709378e41fe79f6"
  val lipsum4Sha256: String = "4fcc40f66613006fe940380962c15356c2902de14ff0d55a7f8c794c8b4376ca"
  val lipsum4Sha512: String = "998801f04fe63e474ccd3a76f72467b7bac2a638ad79ef5427b1fc63426afafe" +
    "e7e3639cba8c3a864fea8a6f58fc0a37310b3f4550466d8ff2c9ba0835bb3231"

  val lipsum5URL: URL = new URL("https://raw.githubusercontent.com/rvanheest-DANS-KNAW/" +
    "dans-bag-lib/c8681a5932e4081dfec95680abefc9a07740a97a/src/test/resources/fetch-files/" +
    "lipsum5.txt")
  val lipsum5Md5: String = "23161a113303da6a76332a102cfd28bb"
  val lipsum5Sha1: String = "0d7c7d44a34f06da37b4bdbca52a62fe27d1d042"
  val lipsum5Sha256: String = "7bc7f1faf0ce3f3703093184360fc6c84d7a271c7fd5f938ea944d4a64e102df"
  val lipsum5Sha512: String = "1aff97b73024b81d3283c44f5b1feb1013fa880909f5fc15cc628903e6297e4f" +
    "0dd002f453a126a4cc1801c3e594db3d13357b81c6a600ad62f073d72bb4f0cf"
}
