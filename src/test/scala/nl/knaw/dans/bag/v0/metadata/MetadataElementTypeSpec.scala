package nl.knaw.dans.bag.v0.metadata

import nl.knaw.dans.bag.fixtures.TestSupportFixture

class MetadataElementTypeSpec extends TestSupportFixture {

  "prefix" should "return the prefix of an element" in {
    MetadataElementType.DC_TITLE.prefix shouldBe "dc"
  }

  "label" should "return the label of an element" in {
    MetadataElementType.DCTERMS_CREATOR.label shouldBe "creator"
  }

  "withName" should "find a MetadataElementType based on a String representation of the prefix and label" in {
    MetadataElementType.withName("dcterms", "audience") shouldBe MetadataElementType.DCTERMS_AUDIENCE
  }

  it should "find a MetadataElementType when using the original withName function from the Enumeration class" in {
    MetadataElementType.withName("dc:coverage") shouldBe MetadataElementType.DC_COVERAGE
  }

  it should "fail when the prefix is not found" in {
    the[NoSuchElementException] thrownBy MetadataElementType.withName("invalid", "audience") should have message "No value found for 'invalid:audience'"
  }

  it should "fail when the label is not found" in {
    the[NoSuchElementException] thrownBy MetadataElementType.withName("dc", "invalid") should have message "No value found for 'dc:invalid'"
  }

  it should "fail when the name is not found" in {
    the[NoSuchElementException] thrownBy MetadataElementType.withName("invalid") should have message "No value found for 'invalid'"
  }
}
