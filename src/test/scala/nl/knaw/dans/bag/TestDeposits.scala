package nl.knaw.dans.bag

import better.files.File
import nl.knaw.dans.bag.v0.{ Deposit, DepositProperties }

import scala.util.{ Failure, Success, Try }

trait TestDeposits extends FileSystemSupport {
  this: TestSupportFixture =>

  protected val minimalDepositPropertiesDir: File = testDir / "minimal-deposit-properties"
  protected val simpleDepositDir: File = testDir / "simple-deposit"

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    val bags = List(
      "/test-deposits/simple-deposit" -> simpleDepositDir,
      "/test-deposits/minimal-deposit-properties" -> minimalDepositPropertiesDir
    )

    for ((src, target) <- bags)
      File(getClass.getResource(src)).copyTo(target)

  }

  protected implicit def removeTry[T](t: Try[T]): T = t match {
    case Success(x) => x
    case Failure(e) => throw e
  }

  protected def minimalDeposit(): Deposit = Deposit.read(minimalDepositPropertiesDir)

  protected def minimalDepositProperties: DepositProperties = {
    DepositProperties.read(minimalDepositPropertiesDir / "deposit.properties")
  }

  protected def simpleDeposit(): Deposit = Deposit.read(simpleDepositDir)

  protected def simpleDepositProperties: DepositProperties = {
    DepositProperties.read(simpleDepositDir / "deposit.properties")
  }
}
