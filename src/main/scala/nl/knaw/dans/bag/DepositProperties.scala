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

import java.nio.file.NoSuchFileException
import java.util.UUID

import better.files.File
import nl.knaw.dans.bag.DepositProperties._
import nl.knaw.dans.bag.SpringfieldPlayMode.SpringfieldPlayMode
import nl.knaw.dans.bag.StageState.StageState
import nl.knaw.dans.bag.StateLabel.StateLabel
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.BooleanUtils
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import scala.util.{ Failure, Try }

case class DepositProperties(creation: Creation = Creation(),
                             state: State,
                             depositor: Depositor,
                             bagStore: BagStore,
                             identifier: Identifier = Identifier(),
                             curation: Curation = Curation(),
                             springfield: Springfield = Springfield(),
                             staged: Staged = Staged()) {

  def save(file: File): Try[Unit] = Try {
    new PropertiesConfiguration {
      setDelimiterParsingDisabled(true)

      setProperty(creationTimestamp, creation.timestampString)

      setProperty(stateLabel, state.label.toString)
      setProperty(stateDescription, state.description)

      setProperty(depositorUserId, depositor.userId)

      setProperty(bagStoreBagId, bagStore.bagId)
      bagStore.archivedString.foreach(setProperty(bagStoreArchived, _))

      identifier.doi.foreach(setProperty(doiIdentifier, _))

      curation.dataManager.userId.foreach(setProperty(dataManagerUserId, _))
      curation.dataManager.email.foreach(setProperty(datamanagerEmail, _))
      curation.isNewVersionString.foreach(setProperty(isNewVersion, _))
      curation.requiredString.foreach(setProperty(curationRequired, _))
      curation.performedString.foreach(setProperty(curationPerformed, _))

      springfield.domain.foreach(setProperty(springfieldDomain, _))
      springfield.user.foreach(setProperty(springfieldUser, _))
      springfield.collection.foreach(setProperty(springfieldCollection, _))
      springfield.playMode.foreach(setProperty(sprinfieldPlaymode, _))

      staged.state.foreach(state => setProperty(stagedState, state.toString))
    }.save(file.toJava)
  }
}

object DepositProperties {

  // @formatter:off
  val creationTimestamp     = "creation.timestamp"
  val stateLabel            = "state.label"
  val stateDescription      = "state.description"
  val depositorUserId       = "depositor.userId"
  val bagStoreBagId         = "bag-store.bag-id"
  val bagStoreArchived      = "bag-store.archived"
  val doiIdentifier         = "identifier.doi"
  val dataManagerUserId     = "curation.datamanager.userId"
  val datamanagerEmail      = "curation.datamanager.email"
  val isNewVersion          = "curation.is-new-version"
  val curationRequired      = "curation.required"
  val curationPerformed     = "curation.performed"
  val springfieldDomain     = "springfield.domain"
  val springfieldUser       = "springfield.user"
  val springfieldCollection = "springfield.collection"
  val sprinfieldPlaymode    = "springfield.playmode"
  val stagedState           = "staged.state"
  // @formatter:on

  def empty(state: State, depositor: Depositor, bagStore: BagStore): DepositProperties = {
    DepositProperties(
      state = state,
      depositor = depositor,
      bagStore = bagStore
    )
  }

  def read(propertiesFile: File): Try[DepositProperties] = {
    if (propertiesFile.exists && propertiesFile.isRegularFile)
      Try {
        new PropertiesConfiguration {
          setDelimiterParsingDisabled(true)
          load(propertiesFile.toJava)
        }
      }.flatMap(load)
    else
      Failure(new NoSuchFileException(s"$propertiesFile does not exist or isn't a file"))
  }

  def load(properties: PropertiesConfiguration): Try[DepositProperties] = Try {
    DepositProperties(
      creation = new Creation(
        timestamp = properties.getString(creationTimestamp)
      ),
      state = new State(
        label = properties.getString(stateLabel),
        description = properties.getString(stateDescription)
      ),
      depositor = Depositor(
        userId = properties.getString(depositorUserId)
      ),
      bagStore = new BagStore(
        bagId = properties.getString(bagStoreBagId),
        archived = properties.getString(bagStoreArchived)
      ),
      identifier = Identifier(
        doi = Option(properties.getString(doiIdentifier))
      ),
      curation = new Curation(
        userId = Option(properties.getString(dataManagerUserId)),
        email = Option(properties.getString(datamanagerEmail)),
        isNewVersion = Option(properties.getString(isNewVersion)),
        required = Option(properties.getString(curationRequired)),
        performed = Option(properties.getString(curationPerformed))
      ),
      springfield = new Springfield(
        domain = properties.getString(springfieldDomain),
        user = properties.getString(springfieldUser),
        collection = properties.getString(springfieldCollection),
        playMode = properties.getString(sprinfieldPlaymode)
      ),
      staged = new Staged(
        state = properties.getString(stagedState)
      )
    )
  }
}

case class Creation(timestamp: DateTime = DateTime.now) {
  def this(timestamp: String) = {
    this(DateTime.parse(timestamp, ISODateTimeFormat.dateTime()))
  }

  def timestampString: String = timestamp.toString(ISODateTimeFormat.dateTime())
}

object StateLabel extends Enumeration {
  type StateLabel = Value

  val DRAFT: StateLabel = Value
  val FINALIZING: StateLabel = Value
  val INVALID: StateLabel = Value
  val SUBMITTED: StateLabel = Value
  val REJECTED: StateLabel = Value
  val FAILED: StateLabel = Value
  val STALLED: StateLabel = Value
  val ARCHIVED: StateLabel = Value
}

case class State(label: StateLabel, description: String) {
  def this(label: String, description: String) = {
    this(StateLabel.withName(label), description)
  }
}

case class Depositor(userId: String)

case class Identifier(doi: Option[String] = None)

case class BagStore(bagId: UUID,
                    archived: Option[Boolean] = None) {
  def this(bagId: UUID, archived: Boolean) = {
    this(bagId, Option(archived))
  }

  def this(bagId: String, archived: String) = {
    this(UUID.fromString(bagId), Option(archived).map(BooleanUtils.toBoolean))
  }

  def archivedString: Option[String] = archived.map(BooleanUtils.toStringYesNo)

  def isArchived: Boolean = archived.getOrElse(false)
}

case class DataManager(userId: Option[String] = Option.empty,
                       email: Option[String] = Option.empty)

case class Curation(dataManager: DataManager = DataManager(),
                    isNewVersion: Option[Boolean] = Option.empty,
                    required: Option[Boolean] = Option.empty,
                    performed: Option[Boolean] = Option.empty) {
  def this(userId: Option[String],
           email: Option[String],
           isNewVersion: Option[String],
           required: Option[String],
           performed: Option[String]) = {
    this(DataManager(userId, email),
      isNewVersion.map(BooleanUtils.toBoolean),
      required.map(BooleanUtils.toBoolean),
      performed.map(BooleanUtils.toBoolean),
    )
  }

  def isNewVersionString: Option[String] = isNewVersion.map(BooleanUtils.toStringYesNo)

  def requiredString: Option[String] = required.map(BooleanUtils.toStringYesNo)

  def performedString: Option[String] = performed.map(BooleanUtils.toStringYesNo)
}

object SpringfieldPlayMode extends Enumeration {
  type SpringfieldPlayMode = Value

  val CONTINUOUS: SpringfieldPlayMode = Value("continuous")
  val MENU: SpringfieldPlayMode = Value("menu")
}

case class Springfield(domain: Option[String] = Option.empty,
                       user: Option[String] = Option.empty,
                       collection: Option[String] = Option.empty,
                       playMode: Option[SpringfieldPlayMode] = Option.empty) {
  def this(domain: String,
           user: String,
           collection: String,
           playMode: String) = {
    this(Option(domain), Option(user), Option(collection), Option(playMode).map(SpringfieldPlayMode.withName))
  }
}

object StageState extends Enumeration {
  type StageState = Value

  val DRAFT: StageState = Value
  val FINALIZING: StageState = Value
  val INVALID: StageState = Value
  val SUBMITTED: StageState = Value
  val REJECTED: StageState = Value
  val FAILED: StageState = Value
  val STALLED: StageState = Value
  val ARCHIVED: StageState = Value
}

case class Staged(state: Option[StageState] = Option.empty) {
  def this(state: String) = this(Option(state).map(StageState.withName))
}
