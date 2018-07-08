package nl.knaw.dans.bag.v0

import java.util.{ Objects, UUID }

import better.files.File
import nl.knaw.dans.bag.v0.ChecksumAlgorithm.ChecksumAlgorithm
import nl.knaw.dans.bag.v0.Deposit._
import nl.knaw.dans.bag.v0.SpringfieldPlayMode.SpringfieldPlayMode
import nl.knaw.dans.bag.v0.StageState.StageState
import nl.knaw.dans.bag.v0.StateLabel.StateLabel
import org.joda.time.DateTime

import scala.util.{ Failure, Success, Try }

class Deposit private(val baseDir: File,
                      val bag: Bag,
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

  def withCreation(timestamp: DateTime): Deposit = {
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

  private val depositPropertiesName = "deposit.properties"

  def empty(baseDir: File,
            algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
            bagInfo: Map[String, Seq[String]] = Map.empty,
            state: State,
            depositor: Depositor,
            bagStore: BagStore): Try[Deposit] = {
    for {
      bag <- Bag.empty(baseDir / bagStore.bagId.toString, algorithms, bagInfo)
      properties = DepositProperties.empty(state, depositor, bagStore)
    } yield new Deposit(baseDir, bag, properties)
  }

  def createFromData(payloadDir: File,
                     algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
                     bagInfo: Map[String, Seq[String]] = Map.empty,
                     state: State,
                     depositor: Depositor,
                     bagStore: BagStore): Try[Deposit] = {
    for {
      bag <- Bag.createFromData(payloadDir, algorithms, bagInfo)
      _ <- moveBag(bag, bagStore.bagId)
      properties = DepositProperties.empty(state, depositor, bagStore)
    } yield new Deposit(payloadDir, bag, properties)
  }

  def read(baseDir: File): Try[Deposit] = {
    for {
      bagDir <- findBagDir(baseDir)
      bag <- Bag.read(bagDir)
      properties <- DepositProperties.read(depositProperties(baseDir))
    } yield new Deposit(baseDir, bag, properties)
  }

  implicit def depositAsFile(deposit: Deposit): File = deposit.baseDir

  private def moveBag(bag: Bag, bagId: UUID): Try[Unit] = Try {
    val depositDir = bag.baseDir.parent
    val newBagDir = depositDir / bagId.toString createDirectory()

    bag.baseDir.moveTo(newBagDir)
  }

  private def findBagDir(baseDir: File): Try[File] = {
    baseDir.list.filter(_.isDirectory).toList match {
      case dir :: Nil => Success(dir)
      case Nil => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains no directories"))
      case _ => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains multiple directories"))
    }
  }

  private def depositProperties(baseDir: File): File = baseDir / depositPropertiesName
}
