package nl.knaw.dans.bag.v0

import java.io.InputStream

import better.files.File
import nl.knaw.dans.bag.RelativePath
import nl.knaw.dans.bag.v0.DcTermsElementType.DcTermsElementType
import nl.knaw.dans.bag.v0.FileAccessCategory.FileAccessCategory
import nl.knaw.dans.bag.v0.XmlLang.XmlLang

import scala.util.Try

/**
 * FilesXml gives a listing of all payload files.
 * the resulting xml file will be valid according to
 * https://easy.dans.knaw.nl/schemas/bag/metadata/files/files.xsd
 */
case class FilesXml() {

  var files : Map[String, FilesXmlItem] = Map()
  def addFilesXmlItem(item: FilesXmlItem) : Unit = {
    files += (item.filepath -> item)
  }

  /**
   * Saves the deserialized FilesXml at the given location
   * @param file
   * @return
   */
  def save(file: File): Try[Unit] = ???

}

object FilesXml {

  def empty(): FilesXml = {
    FilesXml()
  }

  def read(file: File): Try[FilesXml] = ???

  def read(inputStream: InputStream): Try[FilesXml] = ???
}


/**
 *
 * @param pathInData A path in the payload directory of the DansBag
 * @param fileFormat A format from https://www.iana.org/assignments/media-types/media-types.xhtml
 * @param accessibleToRights
 * @param visibleToRights
 */
case class FilesXmlItem(pathInData: RelativePath,
                        fileFormat: FileFormat,
                        accessibleToRights: Option[FileAccessCategory],
                        visibleToRights: Option[FileAccessCategory],
                        dcelements: Set[DcTermsElement] = Set()
                       ) {

  val filepath: String = pathInData.toString()

}
case class FileFormat(fileFormat: String)

case class DcTermsElement(tag: DcTermsElementType, value: String, language: Option[XmlLang] = None)



object FileAccessCategory extends Enumeration {
  type FileAccessCategory = Value

  val ANONYMOUS: FileAccessCategory = Value
  val RESTRICTED_GROUP: FileAccessCategory = Value
  val RESTRICTED_REQUEST: FileAccessCategory = Value
  val KNOWN: FileAccessCategory = Value
  val NONE: FileAccessCategory = Value
}

//subset of the ISO 639-2 language codes
object XmlLang extends Enumeration {
  type XmlLang = Value

  val ENG: XmlLang = Value
  val NLD: XmlLang = Value
  val FRA: XmlLang = Value
  val DEU: XmlLang = Value
}

object DcTermsElementType extends Enumeration {
  type DcTermsElementType = Value

  //dc terms:
  val ABSTRACT: DcTermsElementType = Value
  val ACCESS_RIGHTS: DcTermsElementType = Value("accessRights")
  val ACCRUAL_METHOD: DcTermsElementType = Value("accrualMethod")
  val ACCRUAL_PERIODICITY: DcTermsElementType = Value("accrualPeriodicity")
  val ACCRUAL_POLICY: DcTermsElementType = Value("accrualPolicy")
  val ALTERNATIVE: DcTermsElementType = Value
  val AUDIENCE: DcTermsElementType = Value
  val AVAILABLE: DcTermsElementType = Value
  val BIBLIOGRAPHIC_CITATION: DcTermsElementType = Value("bibliographicCitation")
  val CONFORMS_TO: DcTermsElementType = Value("conformsTo")
//  val CONTRIBUTOR: DcTermsElementType = Value
//  val COVERAGE: DcTermsElementType = Value
  val CREATED: DcTermsElementType = Value
//  val CREATOR: DcTermsElementType = Value
//  val DATE: DcTermsElementType = Value
  val DATE_ACCEPTED: DcTermsElementType = Value("dateAccepted")
  val DATE_COPYRIGHTED: DcTermsElementType = Value("dateCopyrighted")
  val DATE_SUBMITTED: DcTermsElementType = Value("dateSubmitted")
//  val DESCRIPTION: DcTermsElementType = Value
  val EDUCATION_LEVEL: DcTermsElementType = Value("educationLevel")
  val EXTENT: DcTermsElementType = Value
//  val FORMAT: DcTermsElementType = Value
  val HAS_FORMAT: DcTermsElementType = Value("hasFormat")
  val HAS_PART: DcTermsElementType = Value("hasPart")
  val HAS_VERSION: DcTermsElementType = Value("hasVersion")
//  val IDENTIFIER: DcTermsElementType = Value
  val INSTRUCTIONAL_METHOD: DcTermsElementType = Value("instructionalMethod")
  val IS_FORMAT_OF: DcTermsElementType = Value("isFormatOf")
  val IS_PART_OF: DcTermsElementType = Value("isPartOf")
  val IS_REFERENCED_BY: DcTermsElementType = Value("isReferencedBy")
  val IS_REPLACED_BY: DcTermsElementType = Value("isReplacedBy")
  val IS_REQUIRED_BY: DcTermsElementType = Value("isRequiredBy")
  val ISSUED: DcTermsElementType = Value
  val IS_VERSION_OF: DcTermsElementType = Value("isVersionOf")
//  val LANGUAGE: DcTermsElementType = Value
  val LICENSE: DcTermsElementType = Value
  val MEDIATOR: DcTermsElementType = Value
  val MEDIUM: DcTermsElementType = Value
  val MODIFIED: DcTermsElementType = Value
  val PROVENANCE: DcTermsElementType = Value
//  val PUBLISHER: DcTermsElementType = Value
  val REFERENCES: DcTermsElementType = Value
//  val RELATION: DcTermsElementType = Value
  val REPLACES: DcTermsElementType = Value
  val REQUIRES: DcTermsElementType = Value
//  val RIGHTS: DcTermsElementType = Value
  val RIGHTS_HOLDER: DcTermsElementType = Value("rightsHolder")
//  val SOURCE: DcTermsElementType = Value
  val SPATIAL: DcTermsElementType = Value
//  val SUBJECT: DcTermsElementType = Value
  val TABLE_OF_CONTENTS: DcTermsElementType = Value("tableOfContent")
  val TEMPORAL: DcTermsElementType = Value
//  val TITLE: DcTermsElementType = Value
//  val TYPE: DcTermsElementType = Value
  val VALID: DcTermsElementType = Value
  //dc:
  val CONTRIBUTOR: DcTermsElementType = Value
  val COVERAGE: DcTermsElementType = Value
  val CREATOR: DcTermsElementType = Value
  val DATE: DcTermsElementType = Value
  val DESCRIPTION: DcTermsElementType = Value
  val FORMAT: DcTermsElementType = Value
  val IDENTIFIER: DcTermsElementType = Value
  val LANGUAGE: DcTermsElementType = Value
  val PUBLISHER: DcTermsElementType = Value
  val RELATION: DcTermsElementType = Value
  val RIGHTS: DcTermsElementType = Value
  val SOURCE: DcTermsElementType = Value
  val SUBJECT: DcTermsElementType = Value
  val TITLE: DcTermsElementType = Value
  val TYPE: DcTermsElementType = Value

}
