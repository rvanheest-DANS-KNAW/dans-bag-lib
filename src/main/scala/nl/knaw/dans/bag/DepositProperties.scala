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
import nl.knaw.dans.bag.DepositProperties.{ stateDescription, _ }
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

  /**
   * Writes the `DepositProperties` to `file` on the filesystem.
   *
   * @param file the file location to serialize the properties to
   * @return `scala.util.Success` if the save was performed successfully,
   *         `scala.util.Failure` otherwise
   */
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

  /**
   * Creates a `DepositProperties` object, with only the mandatory properties set.
   *
   * @param state     the `State` to be set
   * @param depositor the accountname of the depositor
   * @param bagStore  the bagId to be used for this deposit
   * @return a new `DepositProperties`
   */
  def from(state: State, depositor: Depositor, bagStore: BagStore): DepositProperties = {
    DepositProperties(
      state = state,
      depositor = depositor,
      bagStore = bagStore
    )
  }

  /**
   * Reads a `File` as a `deposit.properties` file.
   *
   * @param propertiesFile the file to be converted to a `DepositProperties`
   * @return if successful the `DepositProperties` representing the `propertiesFile`,
   *         else a Failure with a NoSuchFileException
   */
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

  /**
   * Loads a new `DepositProperties` object with the corresponding elements from the
   * `PropertiesConfiguration`. `properties` should at least contain all mandatory properties.
   *
   * @param properties the `PropertiesConfiguration` containing at least all mandatory deposit properties
   * @return if successful a new `DepositProperties` representing the provided `properties`
   *         else a `Failure` with a `NoSuchElementException` if not all deposit properties were present
   */
  def load(properties: PropertiesConfiguration): Try[DepositProperties] = Try {
    val creationTimestampValue = properties.getString(creationTimestamp)
    val stateLabelValue = properties.getString(stateLabel)
    val stateDescriptionValue = properties.getString(stateDescription)
    val depositorUserIdValue = properties.getString(depositorUserId)
    val bagStoreBagIdValue = properties.getString(bagStoreBagId)

    require(creationTimestampValue != null, s"could not find mandatory field '$creationTimestamp'")
    require(stateLabelValue != null, s"could not find mandatory field '$stateLabel'")
    require(stateDescriptionValue != null, s"could not find mandatory field '$stateDescription'")
    require(depositorUserIdValue != null, s"could not find mandatory field '$depositorUserId'")
    require(bagStoreBagIdValue != null, s"could not find mandatory field '$bagStoreBagId'")

    DepositProperties(
      creation = new Creation(
        timestamp = creationTimestampValue
      ),
      state = new State(
        label = stateLabelValue,
        description = stateDescriptionValue
      ),
      depositor = Depositor(
        userId = depositorUserIdValue
      ),
      bagStore = new BagStore(
        bagId = bagStoreBagIdValue,
        archived = properties.getString(bagStoreArchived)
      ),
      identifier = new Identifier(
        doi = properties.getString(doiIdentifier)
      ),
      curation = new Curation(
        userId = properties.getString(dataManagerUserId),
        email = properties.getString(datamanagerEmail),
        isNewVersion = properties.getString(isNewVersion),
        required = properties.getString(curationRequired),
        performed = properties.getString(curationPerformed)
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

case class Identifier(doi: Option[String] = None) {
  def this(doi: String) = {
    this(Option(doi))
  }
}

case class BagStore(bagId: UUID,
                    archived: Option[Boolean] = None) {
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
  def this(userId: String,
           email: String,
           isNewVersion: String,
           required: String,
           performed: String) = {
    this(DataManager(Option(userId), Option(email)),
      Option(isNewVersion).map(BooleanUtils.toBoolean),
      Option(required).map(BooleanUtils.toBoolean),
      Option(performed).map(BooleanUtils.toBoolean),
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
