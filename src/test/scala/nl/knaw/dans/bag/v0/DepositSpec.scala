package nl.knaw.dans.bag.v0

import java.util.UUID

import nl.knaw.dans.bag._
import org.joda.time.{ DateTime, DateTimeZone }

import scala.language.implicitConversions
import scala.util.Success

class DepositSpec extends TestSupportFixture
  with FileSystemSupport
  with TestDeposits
  with FixDateTimeNow
  with Lipsum {

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

  "baseDir" should "return the deposit's base directory" in {
    val deposit = simpleDeposit()

    deposit.baseDir shouldBe simpleDepositDir
  }

  "bag" should "return the Deposit's bag object" in {
    val deposit = simpleDeposit()

    deposit.bag.baseDir shouldBe simpleDepositDir / "1c2f78a1-26b8-4a40-a873-1073b9f3a56a"
  }

  "creationTimestamp" should "return the creation timestamp from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.creationTimestamp.getMillis shouldBe new DateTime(2018, 5, 25, 20, 8, 56, 210, DateTimeZone.forOffsetHoursMinutes(2, 0)).getMillis
  }

  "withCreationTimestamp" should "change the creation timestamp and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val timestamp = fixedDateTimeNow

    val resultDeposit = deposit.withCreationTimestamp(timestamp)

    resultDeposit.creationTimestamp shouldBe timestamp
  }

  "stateLabel" should "return the state label from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.stateLabel shouldBe StateLabel.SUBMITTED
  }

  "stateDescription" should "return the state description from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.stateDescription shouldBe "Deposit is valid, and ready for post-submission processing"
  }

  "withState" should "change the state label and description and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val label = StateLabel.ARCHIVED
    val description = "deposit is now archived"

    val resultDeposit = deposit.withState(label, description)

    resultDeposit.stateLabel shouldBe label
    resultDeposit.stateDescription shouldBe description
  }

  "depositor" should "return the userId of the depositor from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.depositor shouldBe "myuser"
  }

  "withDepositor" should "change the depositor's id and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val depositor = "other-depositor"

    val resultDeposit = deposit.withDepositor(depositor)

    resultDeposit.depositor shouldBe depositor
  }

  "bagId" should "return the bagId of the deposit from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.bagId shouldBe UUID.fromString("1c2f78a1-26b8-4a40-a873-1073b9f3a56a")
  }

  "isArchived" should "return the isArchived property from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.isArchived.value shouldBe true
  }

  "withIsArchived" should "change the isArchived property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val isArchived = false

    val resultDeposit = deposit.withIsArchived(isArchived)

    resultDeposit.isArchived.value shouldBe isArchived
  }

  "withoutIsArchived" should "remove the isArchived property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutIsArchived

    resultDeposit.isArchived shouldBe empty
  }

  "doi" should "return the doi of this deposit from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.doi.value shouldBe "some-random-doi"
  }

  "withDoi" should "change the doi of this deposit and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val doi = "other-doi"

    val resultDeposit = deposit.withDoi(doi)

    resultDeposit.doi.value shouldBe doi
  }

  "withoutDoi" should "remove the doi of this deposit and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutDoi

    resultDeposit.doi shouldBe empty
  }

  "dataManagerId" should "return the datamanager's id from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.dataManagerId.value shouldBe "myadmin"
  }

  "withDataManagerId" should "change the datamanager's id and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val datamanagerId = "id"

    val resultDeposit = deposit.withDataManagerId(datamanagerId)

    resultDeposit.dataManagerId.value shouldBe datamanagerId
  }

  "withoutIsArchived" should "remove the datamanager's id and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutIsArchived

    resultDeposit.isArchived shouldBe empty
  }

  "dataManagerEmail" should "return the datamanager's email from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.dataManagerEmail.value shouldBe "FILL.IN.YOUR@VALID-EMAIL.NL"
  }

  "withDataManagerEmail" should "change the datamanager's email and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val datamanagerEmail = "me@email.com"

    val resultDeposit = deposit.withDataManagerEmail(datamanagerEmail)

    resultDeposit.dataManagerEmail.value shouldBe datamanagerEmail
  }

  "withoutDataManagerEmail" should "remove the datamanager's email and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutDataManagerEmail

    resultDeposit.dataManagerEmail shouldBe empty
  }

  "isNewVersion" should "return the isNewVersion property from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.isNewVersion.value shouldBe false
  }

  "withIsNewVersion" should "change the isNewVersion property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val isNewVersion = true

    val resultDeposit = deposit.withIsNewVersion(isNewVersion)

    resultDeposit.isNewVersion.value shouldBe isNewVersion
  }

  "withoutIsNewVersion" should "remove the isNewVersion property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutIsNewVersion

    resultDeposit.isNewVersion shouldBe empty
  }

  "isCurationRequired" should "return the isCurationRequired property from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.isCurationRequired.value shouldBe true
  }

  "withIsCurationRequired" should "change the isCurationRequired property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val required = false

    val resultDeposit = deposit.withIsCurationRequired(required)

    resultDeposit.isCurationRequired.value shouldBe required
  }

  "withoutIsCurationRequired" should "remove the isCurationRequired property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutIsCurationRequired

    resultDeposit.isCurationRequired shouldBe empty
  }

  "isCurationPerformed" should "return the isCurationPerformed property from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.isCurationPerformed.value shouldBe false
  }

  "withIsCurationPerformed" should "change the isCurationPerformed property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val performed = true

    val resultDeposit = deposit.withIsCurationPerformed(performed)

    resultDeposit.isCurationPerformed.value shouldBe performed
  }

  "withoutIsCurationPerformed" should "remove the isCurationPerformed property and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutIsCurationPerformed

    resultDeposit.isCurationPerformed shouldBe empty
  }

  "springfieldDomain" should "return the Springfield domain from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.springfieldDomain.value shouldBe "mydomain"
  }

  "withSpringfieldDomain" should "change the Springfield domain and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val domain = "other-domain"

    val resultDeposit = deposit.withSpringfieldDomain(domain)

    resultDeposit.springfieldDomain.value shouldBe domain
  }

  "withoutSpringfieldDomain" should "remove the Springfield domain and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutSpringfieldDomain

    resultDeposit.springfieldDomain shouldBe empty
  }

  "springfieldUser" should "return the Springfield user from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.springfieldUser.value shouldBe "myname"
  }

  "withSpringfieldUser" should "change the Springfield user and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val user = "other-user"

    val resultDeposit = deposit.withSpringfieldUser(user)

    resultDeposit.springfieldUser.value shouldBe user
  }

  "withoutSpringfieldUser" should "remove the Springfield user and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutSpringfieldUser

    resultDeposit.springfieldUser shouldBe empty
  }

  "springfieldCollection" should "return the Springfield collection from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.springfieldCollection.value shouldBe "my-test-files"
  }

  "withSpringfieldCollection" should "change the Springfield collection and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val collection = "other-collection"

    val resultDeposit = deposit.withSpringfieldCollection(collection)

    resultDeposit.springfieldCollection.value shouldBe collection
  }

  "withoutSpringfieldCollection" should "remove the Springfield collection and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutSpringfieldCollection

    resultDeposit.springfieldCollection shouldBe empty
  }

  "springfieldPlayMode" should "return the Springfield playmode from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.springfieldPlayMode.value shouldBe SpringfieldPlayMode.CONTINUOUS
  }

  "withSpringfieldPlayMode" should "change the Springfield playmode and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val playMode = SpringfieldPlayMode.MENU

    val resultDeposit = deposit.withSpringfieldPlayMode(playMode)

    resultDeposit.springfieldPlayMode.value shouldBe playMode
  }

  "withoutSpringfieldPlayMode" should "remove the Springfield playmode and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutSpringfieldPlayMode

    resultDeposit.springfieldPlayMode shouldBe empty
  }

  "stageState" should "return the stage state from deposit.properties" in {
    val deposit = simpleDeposit()

    deposit.stageState.value shouldBe StageState.ARCHIVED
  }

  "withStageState" should "change the stage state and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val stageState = StageState.DRAFT

    val resultDeposit = deposit.withStageState(stageState)

    resultDeposit.stageState.value shouldBe stageState
  }

  "withoutStageState" should "remove the stage state and return the new DepositProperties" in {
    val deposit = simpleDeposit()
    val resultDeposit = deposit.withoutStageState

    resultDeposit.stageState shouldBe empty
  }

  "save" should "write changes made to the Bag object in the bag to the deposit on file system" in {
    val deposit = simpleDeposit()
    val newFile = testDir / "new-file" writeText lipsum(5)
    val relativeDest: RelativePath = _ / "abc.txt"
    val dest = relativeDest(deposit.bag.data)
    val sha1Checksum = newFile.sha1.toLowerCase

    dest.toJava shouldNot exist
    deposit.bag.payloadManifestAlgorithms should contain only ChecksumAlgorithm.SHA1
    deposit.bag.payloadManifests(ChecksumAlgorithm.SHA1) shouldNot contain key dest

    deposit.bag
      .addPayloadFile(newFile)(_ / "abc.txt")
      .flatMap(_.addPayloadManifestAlgorithm(ChecksumAlgorithm.MD5)) shouldBe a[Success[_]]

    // without saving, the new file should be added to the bag...
    dest.toJava should exist
    // ... and all object structures should be updated accordingly...
    deposit.bag.payloadManifestAlgorithms should contain only(ChecksumAlgorithm.SHA1, ChecksumAlgorithm.MD5)
    deposit.bag.payloadManifests(ChecksumAlgorithm.SHA1) should contain key dest
    deposit.bag.payloadManifests(ChecksumAlgorithm.MD5) should contain key dest
    // ... however, they are not yet persisted to file
    (deposit.bag.baseDir / "manifest-sha1.txt").contentAsString should (
      not include deposit.bag.baseDir.relativize(dest).toString and
        not include sha1Checksum)
    (deposit.bag.baseDir / "manifest-md5.txt").toJava shouldNot exist

    deposit.save() shouldBe a[Success[_]]

    (deposit.bag.baseDir / "manifest-sha1.txt").contentAsString should (
      include(deposit.bag.baseDir.relativize(dest).toString) and
        include(sha1Checksum))
    (deposit.bag.baseDir / "manifest-md5.txt").toJava should exist
  }

  it should "write changes made to the DepositProperties to the deposit on file system" in {
    val stateLabel = StateLabel.ARCHIVED
    val stateDescription = "deposit is archived"
    val doi = "0123456789abcdef"

    simpleDeposit()
      .withState(stateLabel, stateDescription)
      .withDoi(doi)
      .save() shouldBe a[Success[_]]

    val deposit = simpleDeposit()

    deposit.stateLabel shouldBe stateLabel
    deposit.stateDescription shouldBe stateDescription
    deposit.doi.value shouldBe doi
  }
}
