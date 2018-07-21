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

import nl.knaw.dans.bag.fixtures.{ FileSystemSupport, FixDateTimeNow, TestDeposits, TestSupportFixture }
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{ DateTime, DateTimeUtils, DateTimeZone }

import scala.language.implicitConversions
import scala.util.Failure

class DepositPropertiesSpec extends TestSupportFixture with FileSystemSupport with TestDeposits with FixDateTimeNow {

  "empty" should "create a deposit.properties object containing only the minimal required properties" in {
    val state = State(StateLabel.DRAFT, "this deposit is still in draft")
    val depositor = Depositor("myuser")
    val bagId = UUID.fromString("1c2f78a1-26b8-4a40-a873-1073b9f3a56a")
    val bagStore = new BagStore(bagId, archived = false)
    val props = DepositProperties.empty(state, depositor, bagStore)

    props.creation.timestamp.toString(ISODateTimeFormat.dateTime()) shouldBe fixedDateTimeNow.toString(ISODateTimeFormat.dateTime())

    props.state.label shouldBe state.label
    props.state.description shouldBe state.description

    props.depositor.userId shouldBe depositor.userId

    props.bagStore.bagId shouldBe bagId
    props.bagStore.isArchived shouldBe false

    props.identifier.doi shouldBe empty

    props.curation.dataManager.email shouldBe empty
    props.curation.dataManager.userId shouldBe empty
    props.curation.isNewVersion shouldBe empty
    props.curation.required shouldBe empty
    props.curation.performed shouldBe empty

    props.springfield.collection shouldBe empty
    props.springfield.domain shouldBe empty
    props.springfield.user shouldBe empty
    props.springfield.playMode shouldBe empty

    props.staged.state shouldBe empty

    // unset fixed DateTime.now
    DateTimeUtils.setCurrentMillisOffset(0L)
  }

  "read" should "read the properties from a deposit.properties with all properties in use" in {
    val props = simpleDepositPropertiesV0
    props.creation.timestamp.toString(ISODateTimeFormat.dateTime()) shouldBe new DateTime(2018, 5, 25, 20, 8, 56, 210, DateTimeZone.forOffsetHoursMinutes(2, 0)).toString(ISODateTimeFormat.dateTime())

    props.state.label shouldBe StateLabel.SUBMITTED
    props.state.description shouldBe "Deposit is valid, and ready for post-submission processing"

    props.depositor.userId shouldBe "myuser"

    props.bagStore.bagId shouldBe UUID.fromString("1c2f78a1-26b8-4a40-a873-1073b9f3a56a")
    props.bagStore.isArchived shouldBe true

    props.identifier.doi.value shouldBe "some-random-doi"

    props.curation.dataManager.email.value shouldBe "FILL.IN.YOUR@VALID-EMAIL.NL"
    props.curation.dataManager.userId.value shouldBe "myadmin"
    props.curation.isNewVersion.value shouldBe false
    props.curation.required.value shouldBe true
    props.curation.performed.value shouldBe false

    props.springfield.collection.value shouldBe "my-test-files"
    props.springfield.domain.value shouldBe "mydomain"
    props.springfield.user.value shouldBe "myname"
    props.springfield.playMode.value shouldBe SpringfieldPlayMode.CONTINUOUS

    props.staged.state.value shouldBe StageState.ARCHIVED
  }

  it should "read the properties from a deposit.properties with minimal properties" in {
    val props = minimalDepositProperties0
    props.creation.timestamp.toString(ISODateTimeFormat.dateTime()) shouldBe new DateTime(2018, 5, 25, 20, 8, 56, 210, DateTimeZone.forOffsetHoursMinutes(2, 0)).toString(ISODateTimeFormat.dateTime())

    props.state.label shouldBe StateLabel.SUBMITTED
    props.state.description shouldBe "Deposit is valid, and ready for post-submission processing"

    props.depositor.userId shouldBe "myuser"

    props.bagStore.bagId shouldBe UUID.fromString("1c2f78a1-26b8-4a40-a873-1073b9f3a56a")
    props.bagStore.isArchived shouldBe false

    props.identifier.doi shouldBe empty

    props.curation.dataManager.email shouldBe empty
    props.curation.dataManager.userId shouldBe empty
    props.curation.isNewVersion shouldBe empty
    props.curation.required shouldBe empty
    props.curation.performed shouldBe empty

    props.springfield.collection shouldBe empty
    props.springfield.domain shouldBe empty
    props.springfield.user shouldBe empty
    props.springfield.playMode shouldBe empty

    props.staged.state shouldBe empty
  }

  it should "fail if the file does not exist" in {
    val file = simpleDepositDirV0 / "non-existing-deposit.properties"
    inside(DepositProperties.read(file)) {
      case Failure(e: NoSuchFileException) =>
        e should have message s"$file does not exist or isn't a file"
    }
  }

  it should "fail if the given better.files.File is not a regular file" in {
    val file = simpleDepositDirV0
    inside(DepositProperties.read(file)) {
      case Failure(e: NoSuchFileException) =>
        e should have message s"$file does not exist or isn't a file"
    }
  }

  "copy" should "change properties" in {
    val props = simpleDepositPropertiesV0

    val newProps = props.copy(
      state = props.state.copy(label = StateLabel.STALLED),
      bagStore = props.bagStore.copy(archived = None)
    )

    newProps.state.label shouldBe StateLabel.STALLED
    newProps.bagStore.isArchived shouldBe false
  }

  "save" should "write a changed DepositProperties to file" in {
    val props = simpleDepositPropertiesV0

    val newProps = props.copy(
      state = props.state.copy(label = StateLabel.STALLED),
      bagStore = props.bagStore.copy(archived = None)
    )

    val file = simpleDepositDirV0 / "deposit2.properties"
    file.toJava shouldNot exist

    newProps.save(file)
    file.toJava should exist

    val newProps2: DepositProperties = DepositProperties.read(file)
    newProps2 shouldBe newProps

    val file2 = simpleDepositDirV0 / "deposit3.properties"
    file2.toJava shouldNot exist

    newProps2.save(file2)
    file2.toJava should exist

    file.contentAsString shouldBe file2.contentAsString
  }
}
