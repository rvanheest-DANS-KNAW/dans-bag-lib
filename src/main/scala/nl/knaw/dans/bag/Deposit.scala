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

import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException }
import java.util.{ Objects, UUID }

import better.files.File
import nl.knaw.dans.bag.ChecksumAlgorithm.ChecksumAlgorithm
import nl.knaw.dans.bag.Deposit._
import nl.knaw.dans.bag.SpringfieldPlayMode.SpringfieldPlayMode
import nl.knaw.dans.bag.StageState.StageState
import nl.knaw.dans.bag.StateLabel.StateLabel
import org.joda.time.DateTime

import scala.language.{ implicitConversions, postfixOps }
import scala.util.{ Failure, Success, Try }

class Deposit private(val baseDir: File,
                      val bag: DansBag,
                      private val properties: DepositProperties) {

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Deposit =>
        baseDir == that.baseDir &&
          bag == that.bag &&
          properties == that.properties
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hash(baseDir, bag, properties)

  override def toString: String = {
    s"Deposit(baseDir = $baseDir, bag = $bag, properties = $properties)"
  }

  def creationTimestamp: DateTime = properties.creation.timestamp

  def withCreationTimestamp(timestamp: DateTime): Deposit = {
    val newProperties = properties.copy(creation = Creation(timestamp))

    withDepositProperties(newProperties)
  }

  def stateLabel: StateLabel = properties.state.label

  def stateDescription: String = properties.state.description

  def withState(label: StateLabel, description: String): Deposit = {
    val newProperties = properties.copy(state = State(label, description))

    withDepositProperties(newProperties)
  }

  def depositor: String = properties.depositor.userId

  def withDepositor(id: String): Deposit = {
    val newProperties = properties.copy(depositor = Depositor(id))

    withDepositProperties(newProperties)
  }

  def bagId: UUID = properties.bagStore.bagId

  def isArchived: Option[Boolean] = properties.bagStore.archived

  // TODO note that the bagId cannot be changed!
  def withIsArchived(isArchived: Boolean): Deposit = {
    val newProperties = properties.copy(
      bagStore = properties.bagStore.copy(
        archived = Option(isArchived)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutIsArchived: Deposit = {
    val newProperties = properties.copy(
      bagStore = properties.bagStore.copy(
        archived = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def doi: Option[String] = properties.identifier.doi

  def withDoi(doi: String): Deposit = {
    val newProperties = properties.copy(
      identifier = Identifier(Option(doi))
    )

    withDepositProperties(newProperties)
  }

  def withoutDoi: Deposit = {
    val newProperties = properties.copy(identifier = Identifier(Option.empty))

    withDepositProperties(newProperties)
  }

  def dataManagerId: Option[String] = properties.curation.dataManager.userId

  def withDataManagerId(id: String): Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        dataManager = properties.curation.dataManager.copy(
          userId = Option(id)
        )
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutDataManagerId: Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        dataManager = properties.curation.dataManager.copy(
          userId = Option.empty
        )
      )
    )

    withDepositProperties(newProperties)
  }

  def dataManagerEmail: Option[String] = properties.curation.dataManager.email

  def withDataManagerEmail(email: String): Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        dataManager = properties.curation.dataManager.copy(
          email = Option(email)
        )
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutDataManagerEmail: Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        dataManager = properties.curation.dataManager.copy(
          email = Option.empty
        )
      )
    )

    withDepositProperties(newProperties)
  }

  def isNewVersion: Option[Boolean] = properties.curation.isNewVersion

  def withIsNewVersion(isNewVersion: Boolean): Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        isNewVersion = Option(isNewVersion)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutIsNewVersion: Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        isNewVersion = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def isCurationRequired: Option[Boolean] = properties.curation.required

  def withIsCurationRequired(isCurationRequired: Boolean): Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        required = Option(isCurationRequired)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutIsCurationRequired: Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        required = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def isCurationPerformed: Option[Boolean] = properties.curation.performed

  def withIsCurationPerformed(isCurationPerformed: Boolean): Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        performed = Option(isCurationPerformed)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutIsCurationPerformed: Deposit = {
    val newProperties = properties.copy(
      curation = properties.curation.copy(
        performed = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def springfieldDomain: Option[String] = properties.springfield.domain

  def withSpringfieldDomain(domain: String): Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        domain = Option(domain)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutSpringfieldDomain: Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        domain = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def springfieldUser: Option[String] = properties.springfield.user

  def withSpringfieldUser(user: String): Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        user = Option(user)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutSpringfieldUser: Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        user = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def springfieldCollection: Option[String] = properties.springfield.collection

  def withSpringfieldCollection(collection: String): Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        collection = Option(collection)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutSpringfieldCollection: Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        collection = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def springfieldPlayMode: Option[SpringfieldPlayMode] = properties.springfield.playMode

  def withSpringfieldPlayMode(playMode: SpringfieldPlayMode): Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        playMode = Option(playMode)
      )
    )

    withDepositProperties(newProperties)
  }

  def withoutSpringfieldPlayMode: Deposit = {
    val newProperties = properties.copy(
      springfield = properties.springfield.copy(
        playMode = Option.empty
      )
    )

    withDepositProperties(newProperties)
  }

  def stageState: Option[StageState] = properties.staged.state

  def withStageState(state: StageState): Deposit = {
    val newProperties = properties.copy(staged = Staged(Some(state)))

    withDepositProperties(newProperties)
  }

  def withoutStageState: Deposit = {
    val newProperties = properties.copy(staged = Staged())

    withDepositProperties(newProperties)
  }

  /**
   * Saves the in-memory deposit to the file system. It saves all the bag-it files by calling
   * `DansBag.save()` and the deposit.properties by calling `DepositProperties.save()`.
   * As most methods in this library only manipulate the bag-it files and Deposit in memory, a call
   * to `save()` is necessary to serialize the `nl.knaw.dans.bag.Deposit`.
   *
   * @return `scala.util.Success` if the save was performed successfully,
   *         `scala.util.Failure` otherwise
   */
  def save(): Try[Unit] = {
    for {
      _ <- bag.save()
      _ <- properties.save(baseDir / depositPropertiesName)
    } yield ()
  }

  protected def withDepositProperties(depositProperties: DepositProperties): Deposit = {
    new Deposit(this.baseDir, this.bag, depositProperties)
  }
}
object Deposit {

  val depositPropertiesName = "deposit.properties"

  /**
   * Creates a new `nl.knaw.dans.bag.Deposit` with an empty `nl.knaw.dans.bag.DansBag` inside.
   *
   * @param baseDir    the directory for the new Deposit
   * @param algorithms the algorithms with which the checksums for the (payload/tag) files are
   *                   calculated. If none are provided, SHA1 is used.
   * @param bagInfo    the entries to be added to `bag-info.txt`
   * @param state      the state to be set in the `deposit.properties`' state.label
   * @param depositor  the depositor to be set in the `deposit.properties`' `depositor.userId`
   * @param bagStore   the bagId for the `bag-dir` in this `nl.knaw.dans.bag.Deposit`
   * @return if successful, returns a `nl.knaw.dans.bag.Deposit` object representing the deposit
   *         located at `baseDir`, else returns an exception
   */
  def from(baseDir: File,
           algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
           bagInfo: Map[String, Seq[String]] = Map.empty,
           state: State,
           depositor: Depositor,
           bagStore: BagStore): Try[Deposit] = {
    if (baseDir.exists)
      Failure(new FileAlreadyExistsException(baseDir.toString))
    else
      for {
        bag <- DansBag.empty(baseDir / bagStore.bagId.toString, algorithms, bagInfo)
        properties = DepositProperties.from(state, depositor, bagStore)
        _ <- properties.save(depositProperties(baseDir))
      } yield new Deposit(baseDir, bag, properties)
  }

  /**
   * Creates a new Deposit, as a parent-directory to the `payloadDir`. A new `DansBag` will be
   * created in the `Deposit`, with the bag-it files, and the data files in the `data/` directory.
   * However, no `metadata/` files will be created. These have to be added separately.
   *
   * @param payloadDir the directory containing the payload (data) files for the bag. The `Deposit`
   *                   will be created here, and the payload files will be moved to the `data/`
   *                   directory in the new `DansBag`
   * @param algorithms the algorithms with which the checksums for the (payload/tag) files are
   *                   calculated. If none provided SHA1 is used.
   * @param bagInfo    the entries to be added to `bag-info.txt`
   * @param state      the state to be set in the deposit.properties' state.label
   * @param depositor  the depositor to be set in the deposit.properties' depositor.userId
   * @param bagStore   the BagStore containing the target bag-store and the bagId for the bag-dir in
   *                   this Deposit
   * @return if successful, returns a `nl.knaw.dans.bag.Deposit` object representing the deposit
   *         located at `payloadDir` else returns an exception
   */
  def createFromData(payloadDir: File,
                     algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
                     bagInfo: Map[String, Seq[String]] = Map.empty,
                     state: State,
                     depositor: Depositor,
                     bagStore: BagStore): Try[Deposit] = {
    if (payloadDir.notExists)
      Failure(new NoSuchFileException(payloadDir.toString))
    else
      for {
        bagDir <- moveBag(payloadDir, bagStore.bagId)
        bag <- DansBag.createFromData(bagDir, algorithms, bagInfo)
        properties = DepositProperties.from(state, depositor, bagStore)
        _ <- properties.save(depositProperties(payloadDir))
      } yield new Deposit(payloadDir, bag, properties)
  }

  /**
   * Create a deposit based on an already existing bag. If the `bagDir` does not exist or does not
   * represent a valid bag, a `Failure` is returned.
   *
   * @param bagDir the directory containing the bag. The `Deposit` will be created here, and the bag
   *               will be moved to the bag directory within the deposit
   * @param state the state to be set in the deposit.properties' state.label
   * @param depositor the depositor to be set in the deposit.properties' depositor.userId
   * @param bagStore the BagStore containing the target bag-store and the bagId for the bag-dir in
   *                 this Deposit
   * @return if successful, returns a `nl.knaw.dans.bag.Deposit` object representing the deposit
   *         located at `bagDir`, else returns an exception
   */
  def createFromBag(bagDir: File,
                    state: State,
                    depositor: Depositor,
                    bagStore: BagStore): Try[Deposit] = {
    if (bagDir.notExists)
      Failure(new NoSuchFileException(bagDir.toString()))
    else
      for {
        newBagDir <- moveBag(bagDir, bagStore.bagId)
        bag <- DansBag.read(newBagDir)
        properties = DepositProperties.from(state, depositor, bagStore)
        _ <- properties.save(depositProperties(bagDir))
      } yield new Deposit(bagDir, bag, properties)
  }

  /**
   * Reads the `baseDir` as a Deposit.
   *
   * @param baseDir the directory containing the deposit
   * @return if successful, returns a `nl.knaw.dans.bag.Deposit` object representing the deposit
   *         located at `baseDir` else returns an exception
   */
  def read(baseDir: File): Try[Deposit] = {
    for {
      bagDir <- findBagDir(baseDir)
      bag <- DansBag.read(bagDir)
      properties <- DepositProperties.read(depositProperties(baseDir))
    } yield new Deposit(baseDir, bag, properties)
  }

  /**
   * Returns the `baseDir` of the Deposit object
   *
   * @param deposit the deposit to extract the `baseDir` from
   * @return returns the `baseDir` of the Deposit object
   */
  implicit def depositAsFile(deposit: Deposit): File = deposit.baseDir

  private def moveBag(payloadDir: File, bagId: UUID): Try[File] = Try {
    val bagDir = payloadDir / bagId.toString createDirectory()
    for (payloadFile <- payloadDir.list
         if payloadFile != bagDir)
      payloadFile.moveTo(bagDir / payloadDir.relativize(payloadFile).toString)

    bagDir
  }

  private def findBagDir(baseDir: File): Try[File] = {
    // due to backwards compatibility, we cannot implement this using the bagId from deposit.properties
    baseDir.list.filter(_.isDirectory).toList match {
      case dir :: Nil => Success(dir)
      case Nil => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains no directories"))
      case _ => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains multiple directories"))
    }
  }

  private def depositProperties(baseDir: File): File = baseDir / depositPropertiesName
}
