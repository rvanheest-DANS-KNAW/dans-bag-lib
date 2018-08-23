package nl.knaw.dans.bag.v0.metadata.file

import nl.knaw.dans.bag.fixtures.{ TestBags, TestSupportFixture }
import nl.knaw.dans.bag.v0.metadata.files.{ FileAccessCategory, FilesXmlItem }
import nl.knaw.dans.bag.v0.metadata.{ MetadataElement, MetadataElementType, XmlLang }

class FilesXmlItemSpec extends TestSupportFixture with TestBags {

  private val file1: FilesXmlItem = {
    val file = simpleBagDirV0 / "data" / "x"
    val mimetype = "text/plain"
    val accessibleToRights = FileAccessCategory.RESTRICTED_REQUEST
    val visibleToRights = FileAccessCategory.ANONYMOUS
    val metadataElements = Seq(
      MetadataElement(
        tag = MetadataElementType.DC_TITLE,
        value = "my title"
      ),
      MetadataElement(
        tag = MetadataElementType.DCTERMS_DESCRIPTION,
        value = "my description",
        language = Some(XmlLang.English)
      )
    )
    FilesXmlItem(file, mimetype, Some(accessibleToRights), Some(visibleToRights), metadataElements)
  }

  private val file2: FilesXmlItem = {
    val file = simpleBagDirV0 / "data" / "y"
    val mimetype = "text/plain"
    FilesXmlItem(file, mimetype)
  }

  "filepath" should "get the filepath from the FilesXmlItem" in {
    file1.filepath shouldBe simpleBagDirV0 / "data" / "x"
  }

  "mimetype" should "get the mimetype from the FilesXmlItem" in {
    file1.mimetype shouldBe "text/plain"
  }

  "accessibleToRights" should "get the accessibleToRights from the FilesXmlItem" in {
    file1.accessibleToRights.value shouldBe FileAccessCategory.RESTRICTED_REQUEST
  }

  it should "get None when the accessibleToRights is not present" in {
    file2.accessibleToRights shouldBe empty
  }

  "visibleToRights" should "get the visibleToRights from the FilesXmlItem" in {
    file1.visibleToRights.value shouldBe FileAccessCategory.ANONYMOUS
  }

  it should "get None when the visibleToRights is not present" in {
    file2.visibleToRights shouldBe empty
  }

  "metadataElements" should "get the metadataElements listing from the FilesXmlItem" in {
    file1.metadataElements should contain only(
      MetadataElement(
        tag = MetadataElementType.DC_TITLE,
        value = "my title"
      ),
      MetadataElement(
        tag = MetadataElementType.DCTERMS_DESCRIPTION,
        value = "my description",
        language = Some(XmlLang.English)
      )
    )
  }

  it should "get an empty listing when no metadataElements were provided" in {
    file2.metadataElements shouldBe empty
  }

  // TODO continue testing
  // but probably we should first change 'filepath: File' to 'path: Path' to make it a relative path
}
