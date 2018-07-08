package nl.knaw.dans.bag.v0

import better.files.File
import nl.knaw.dans.bag.{ FileSystemSupport, TestSupportFixture }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

class DepositSpec extends TestSupportFixture with FileSystemSupport {

  private val minimalDepositPropertiesDir: File = testDir / "minimal-deposit-properties"
  private val simpleDepositDir: File = testDir / "simple-deposit"

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val bags = "/test-deposits/minimal-deposit-properties" -> minimalDepositPropertiesDir ::
      "/test-deposits/simple-deposit" -> simpleDepositDir ::
      Nil

    for ((src, target) <- bags)
      File(getClass.getResource(src)).copyTo(target)
  }

  private implicit def removeTry(deposit: Try[Deposit]): Deposit = deposit match {
    case Success(x) => x
    case Failure(e) => throw e
  }

  def minimalDepositPropertiesDeposit(): Deposit = Deposit.read(minimalDepositPropertiesDir).recover { case e => throw e }

  def simpleDeposit(): Deposit = Deposit.read(simpleDepositDir)

  "empty" should "create a deposit directory with a bag directory named after the bagId" in pending

  it should "create a deposit directory with a bag directory in it, equivalent to calling Bag.empty" in pending

  it should "create a deposit directory with a deposit.properties file in it" in pending

  it should "create a deposit directory with a deposit.properties file in it, containing the given, basic properties" in pending

  it should "return a Deposit object with the base directory, Bag object and DepositProperties object in it" in pending

  "createFromData" should "create a deposit directory with a bag directory named after the bagId" in pending

  it should "create a deposit directory with a bag directory containing a data directory with " +
    "all data that was in the given base directory" in pending

  it should "create a deposit directory with a deposit.properties file in it" in pending

  it should "create a deposit directory with a deposit.properties file in it, containing the given, basic properties" in pending

  it should "return a Deposit object with the base directory, Bag object and DepositProperties object in it" in pending

  "read" should "load a valid deposit on filesystem into a Deposit object" in pending

  it should "fail when a deposit does not contain a directory (which might classify as a bag)" in pending

  it should "fail when a deposit contains multiple directories (which might classify as a bag)" in pending

  "baseDir" should "return the deposit's base directory" in pending

  "bag" should "return the Deposit's bag object" in pending

  "creation" should "return the creation timestamp from deposit.properties" in pending

  "withCreation" should "change the creation timestamp and return the new DepositProperties" in pending

  "stateLabel" should "return the state label from deposit.properties" in pending

  "stateDescription" should "return the state description from deposit.properties" in pending

  "withState" should "change the state label and description and return the new DepositProperties" in pending

  "depositor" should "return the userId of the depositor from deposit.properties" in pending

  "withDepositor" should "change the depositor's id and return the new DepositProperties" in pending

  "bagId" should "return the bagId of the deposit from deposit.properties" in pending

  "isArchived" should "return the isArchived property from deposit.properties" in pending

  "withIsArchived" should "change the isArchived property and return the new DepositProperties" in pending

  "withoutIsArchived" should "remove the isArchived property and return the new DepositProperties" in pending

  "doi" should "return the doi of this deposit from deposit.properties" in pending

  "withDoi" should "change the doi of this deposit and return the new DepositProperties" in pending

  "withoutDoi" should "remove the doi of this deposit and return the new DepositProperties" in pending

  "dataManagerId" should "return the datamanager's id from deposit.properties" in pending

  "withDataManagerId" should "change the datamanager's id and return the new DepositProperties" in pending

  "withoutIsArchived" should "remove the datamanager's id and return the new DepositProperties" in pending

  "dataManagerEmail" should "return the datamanager's email from deposit.properties" in pending

  "withDataManagerEmail" should "change the datamanager's email and return the new DepositProperties" in pending

  "withoutDataManagerEmail" should "remove the datamanager's email and return the new DepositProperties" in pending

  "isNewVersion" should "return the isNewVersion property from deposit.properties" in pending

  "withIsNewVersion" should "change the isNewVersion property and return the new DepositProperties" in pending

  "withoutIsNewVersion" should "remove the isNewVersion property and return the new DepositProperties" in pending

  "isCurationRequired" should "return the isCurationRequired property from deposit.properties" in pending

  "withIsCurationRequired" should "change the isCurationRequired property and return the new DepositProperties" in pending

  "withoutIsCurationRequired" should "remove the isCurationRequired property and return the new DepositProperties" in pending

  "isCurationPerformed" should "return the isCurationPerformed property from deposit.properties" in pending

  "withIsCurationPerformed" should "change the isCurationPerformed property and return the new DepositProperties" in pending

  "withoutIsCurationPerformed" should "remove the isCurationPerformed property and return the new DepositProperties" in pending

  "springfieldDomain" should "return the Springfield domain from deposit.properties" in pending

  "withSpringfieldDomain" should "change the Springfield domain and return the new DepositProperties" in pending

  "withoutSpringfieldDomain" should "remove the Springfield domain and return the new DepositProperties" in pending

  "springfieldUser" should "return the Springfield user from deposit.properties" in pending

  "withSpringfieldUser" should "change the Springfield user and return the new DepositProperties" in pending

  "withoutSpringfieldUser" should "remove the Springfield user and return the new DepositProperties" in pending

  "springfieldCollection" should "return the Springfield collection from deposit.properties" in pending

  "withSpringfieldCollection" should "change the Springfield collection and return the new DepositProperties" in pending

  "withoutSpringfieldCollection" should "remove the Springfield collection and return the new DepositProperties" in pending

  "springfieldPlayMode" should "return the Springfield playmode from deposit.properties" in pending

  "withSpringfieldPlayMode" should "change the Springfield playmode and return the new DepositProperties" in pending

  "withoutSpringfieldPlayMode" should "remove the Springfield playmode and return the new DepositProperties" in pending

  "stageState" should "return the stage state from deposit.properties" in pending

  "withStageState" should "change the stage state and return the new DepositProperties" in pending

  "withoutStageState" should "remove the stage state and return the new DepositProperties" in pending

  "save" should "write changes made to the Bag object in the bag to the deposit on file system" in pending

  it should "write changes made to the DepositProperties to the deposit on file system" in pending
}
