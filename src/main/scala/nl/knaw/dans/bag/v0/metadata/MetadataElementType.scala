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
package nl.knaw.dans.bag.v0.metadata

object MetadataElementType extends Enumeration {
  private def toName(prefix: String, label: String): String = s"$prefix:$label"

  case class ElementType(prefix: String, label: String)
    extends super.Val(nextId, toName(prefix, label))

  def withName(prefix: String, label: String): MetadataElementType = {
    withName(toName(prefix, label)).asInstanceOf[MetadataElementType]
  }

  type MetadataElementType = ElementType

  // TODO with this setup we cannot distinguish between dcterms and dc variants of the elements
  // does this for example mean that a dcterms:title will be converted on '.save()' to dc:title?

  // @formatter:off
  //dc terms:
  val DCTERMS_ABSTRACT               : MetadataElementType = ElementType("dcterms", "abstract")
  val DCTERMS_ACCESS_RIGHTS          : MetadataElementType = ElementType("dcterms", "accessRights")
  val DCTERMS_ACCRUAL_METHOD         : MetadataElementType = ElementType("dcterms", "accrualMethod")
  val DCTERMS_ACCRUAL_PERIODICITY    : MetadataElementType = ElementType("dcterms", "accrualPeriodicity")
  val DCTERMS_ACCRUAL_POLICY         : MetadataElementType = ElementType("dcterms", "accrualPolicy")
  val DCTERMS_ALTERNATIVE            : MetadataElementType = ElementType("dcterms", "alternative")
  val DCTERMS_AUDIENCE               : MetadataElementType = ElementType("dcterms", "audience")
  val DCTERMS_AVAILABLE              : MetadataElementType = ElementType("dcterms", "available")
  val DCTERMS_BIBLIOGRAPHIC_CITATION : MetadataElementType = ElementType("dcterms", "bibliographicCitation")
  val DCTERMS_CONFORMS_TO            : MetadataElementType = ElementType("dcterms", "conformsTo")
  val DCTERMS_CONTRIBUTOR            : MetadataElementType = ElementType("dcterms", "contributor")
  val DCTERMS_COVERAGE               : MetadataElementType = ElementType("dcterms", "coverage")
  val DCTERMS_CREATED                : MetadataElementType = ElementType("dcterms", "created")
  val DCTERMS_CREATOR                : MetadataElementType = ElementType("dcterms", "creator")
  val DCTERMS_DATE                   : MetadataElementType = ElementType("dcterms", "date")
  val DCTERMS_DATE_ACCEPTED          : MetadataElementType = ElementType("dcterms", "dateAccepted")
  val DCTERMS_DATE_COPYRIGHTED       : MetadataElementType = ElementType("dcterms", "dateCopyrighted")
  val DCTERMS_DATE_SUBMITTED         : MetadataElementType = ElementType("dcterms", "dateSubmitted")
  val DCTERMS_DESCRIPTION            : MetadataElementType = ElementType("dcterms", "description")
  val DCTERMS_EDUCATION_LEVEL        : MetadataElementType = ElementType("dcterms", "educationLevel")
  val DCTERMS_EXTENT                 : MetadataElementType = ElementType("dcterms", "extent")
  val DCTERMS_FORMAT                 : MetadataElementType = ElementType("dcterms", "format")
  val DCTERMS_HAS_FORMAT             : MetadataElementType = ElementType("dcterms", "hasFormat")
  val DCTERMS_HAS_PART               : MetadataElementType = ElementType("dcterms", "hasPart")
  val DCTERMS_HAS_VERSION            : MetadataElementType = ElementType("dcterms", "hasVersion")
  val DCTERMS_IDENTIFIER             : MetadataElementType = ElementType("dcterms", "identifier")
  val DCTERMS_INSTRUCTIONAL_METHOD   : MetadataElementType = ElementType("dcterms", "instructionalMethod")
  val DCTERMS_IS_FORMAT_OF           : MetadataElementType = ElementType("dcterms", "isFormatOf")
  val DCTERMS_IS_PART_OF             : MetadataElementType = ElementType("dcterms", "isPartOf")
  val DCTERMS_IS_REFERENCED_BY       : MetadataElementType = ElementType("dcterms", "isReferencedBy")
  val DCTERMS_IS_REPLACED_BY         : MetadataElementType = ElementType("dcterms", "isReplacedBy")
  val DCTERMS_IS_REQUIRED_BY         : MetadataElementType = ElementType("dcterms", "isRequiredBy")
  val DCTERMS_ISSUED                 : MetadataElementType = ElementType("dcterms", "issued")
  val DCTERMS_IS_VERSION_OF          : MetadataElementType = ElementType("dcterms", "isVersionOf")
  val DCTERMS_LANGUAGE               : MetadataElementType = ElementType("dcterms", "language")
  val DCTERMS_LICENSE                : MetadataElementType = ElementType("dcterms", "license")
  val DCTERMS_MEDIATOR               : MetadataElementType = ElementType("dcterms", "mediator")
  val DCTERMS_MEDIUM                 : MetadataElementType = ElementType("dcterms", "medium")
  val DCTERMS_MODIFIED               : MetadataElementType = ElementType("dcterms", "modified")
  val DCTERMS_PROVENANCE             : MetadataElementType = ElementType("dcterms", "provenance")
  val DCTERMS_PUBLISHER              : MetadataElementType = ElementType("dcterms", "publisher")
  val DCTERMS_REFERENCES             : MetadataElementType = ElementType("dcterms", "references")
  val DCTERMS_RELATION               : MetadataElementType = ElementType("dcterms", "relation")
  val DCTERMS_REPLACES               : MetadataElementType = ElementType("dcterms", "replaces")
  val DCTERMS_REQUIRES               : MetadataElementType = ElementType("dcterms", "requires")
  val DCTERMS_RIGHTS                 : MetadataElementType = ElementType("dcterms", "rights")
  val DCTERMS_RIGHTS_HOLDER          : MetadataElementType = ElementType("dcterms", "rightsHolder")
  val DCTERMS_SOURCE                 : MetadataElementType = ElementType("dcterms", "source")
  val DCTERMS_SPATIAL                : MetadataElementType = ElementType("dcterms", "spatial")
  val DCTERMS_SUBJECT                : MetadataElementType = ElementType("dcterms", "subject")
  val DCTERMS_TABLE_OF_CONTENTS      : MetadataElementType = ElementType("dcterms", "tableOfContent")
  val DCTERMS_TEMPORAL               : MetadataElementType = ElementType("dcterms", "temporal")
  val DCTERMS_TITLE                  : MetadataElementType = ElementType("dcterms", "title")
  val DCTERMS_TYPE                   : MetadataElementType = ElementType("dcterms", "type")
  val DCTERMS_VALID                  : MetadataElementType = ElementType("dcterms", "valid")

  //dc:
  val DC_CONTRIBUTOR : MetadataElementType = ElementType("dc", "contributor")
  val DC_COVERAGE    : MetadataElementType = ElementType("dc", "coverage")
  val DC_CREATOR     : MetadataElementType = ElementType("dc", "created")
  val DC_DATE        : MetadataElementType = ElementType("dc", "date")
  val DC_DESCRIPTION : MetadataElementType = ElementType("dc", "description")
  val DC_FORMAT      : MetadataElementType = ElementType("dc", "format")
  val DC_IDENTIFIER  : MetadataElementType = ElementType("dc", "identifier")
  val DC_LANGUAGE    : MetadataElementType = ElementType("dc", "language")
  val DC_PUBLISHER   : MetadataElementType = ElementType("dc", "publisher")
  val DC_RELATION    : MetadataElementType = ElementType("dc", "relation")
  val DC_RIGHTS      : MetadataElementType = ElementType("dc", "rights")
  val DC_SOURCE      : MetadataElementType = ElementType("dc", "source")
  val DC_SUBJECT     : MetadataElementType = ElementType("dc", "subject")
  val DC_TITLE       : MetadataElementType = ElementType("dc", "title")
  val DC_TYPE        : MetadataElementType = ElementType("dc", "type")
  // @formatter:on
}
