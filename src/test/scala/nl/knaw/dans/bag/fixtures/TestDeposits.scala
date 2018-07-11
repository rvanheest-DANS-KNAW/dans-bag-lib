package nl.knaw.dans.bag.fixtures

import better.files.File
import nl.knaw.dans.bag.{ Deposit, DepositProperties }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

trait TestDeposits extends FileSystemSupport {
  this: TestSupportFixture =>

  protected val minimalDepositPropertiesDirV0: File = testDir / "minimal-deposit-properties"
  protected val simpleDepositDirV0: File = testDir / "simple-deposit"

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val bags = List(
      "/test-deposits/v0/simple-deposit" -> simpleDepositDirV0,
      "/test-deposits/v0/minimal-deposit-properties" -> minimalDepositPropertiesDirV0
    )

    for ((src, target) <- bags)
      File(getClass.getResource(src)).copyTo(target)
  }

  protected implicit def removeTryDeposit(deposit: Try[Deposit]): Deposit = deposit match {
    case Success(x) => x
    case Failure(e) => throw e
  }

  protected implicit def removeTryDepositProperties(props: Try[DepositProperties]): DepositProperties = props match {
    case Success(x) => x
    case Failure(e) => throw e
  }

  protected def minimalDepositV0(): Deposit = Deposit.read(minimalDepositPropertiesDirV0)

  protected def minimalDepositProperties0: DepositProperties = {
    DepositProperties.read(minimalDepositPropertiesDirV0 / "deposit.properties")
  }

  protected def simpleDepositV0(): Deposit = Deposit.read(simpleDepositDirV0)

  protected def simpleDepositPropertiesV0: DepositProperties = {
    DepositProperties.read(simpleDepositDirV0 / "deposit.properties")
  }
}
