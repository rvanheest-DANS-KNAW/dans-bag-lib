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
package nl.knaw.dans.bag.v0

import java.net.URI
import java.util.UUID

import nl.knaw.dans.bag.fixtures.{ TestBags, TestSupportFixture }
import org.joda.time.{ DateTime, DateTimeZone }

import scala.util.{ Failure, Success }

class BagInfoSpecs extends TestSupportFixture with TestBags {

  "bagInfo" should "read the data from bag-info.txt" in {
    simpleBagV0().bagInfo should contain only(
      "Payload-Oxum" -> List("72.6"),
      "Bagging-Date" -> List("2016-06-07"),
      "Bag-Size" -> List("0.6 KB"),
      "Created" -> List("2017-01-16T14:35:00.888+01:00"),
    )
  }

  it should "group equivalent keys from bag-info.txt" in {
    multipleKeysBagV0().bagInfo should contain("Bagging-Date" -> List("2016-06-07", "2016-06-08"))
  }

  "addBagInfo" should "add a new key-value pair to the bag-info" in {
    val bag = simpleBagV0()
    val key = "My-Test-Value"
    val value = "hello world"

    val resultBag = bag.addBagInfo(key, value)

    resultBag.bagInfo should contain(key -> List(value))
  }

  it should "add a new value to an already existing key" in {
    val bag = simpleBagV0()
    val key = "Bagging-Date"
    val oldDate = "2016-06-07"

    bag.bagInfo should contain(key -> List(oldDate))

    val newDate = "2018-01-01"
    val resultBag = bag.addBagInfo(key, newDate)

    resultBag.bagInfo should contain(key -> List(oldDate, newDate))
  }

  "removeBagInfo" should "remove a key-value pair from the bag-info" in {
    val bag = simpleBagV0()
    val createdKey = "Created"

    bag.bagInfo should contain key createdKey

    val resultBag = bag.removeBagInfo(createdKey)

    resultBag.bagInfo shouldNot contain key createdKey
  }

  it should "remove all values when the key contains multiple values" in {
    val bag = multipleKeysBagV0()
    val baggingDateKey = "Bagging-Date"

    bag.bagInfo should contain key baggingDateKey
    bag.bagInfo(baggingDateKey) should have length 2

    val resultBag = bag.removeBagInfo(baggingDateKey)

    resultBag.bagInfo shouldNot contain key baggingDateKey
  }

  it should "ignore the removal of a key that is not present in the bagInfo" in {
    val bag = simpleBagV0()
    val nonExistingKey = "Non-Existing-Key"

    bag.bagInfo shouldNot contain key nonExistingKey

    val resultBag = bag.removeBagInfo(nonExistingKey)

    resultBag.bagInfo shouldNot contain key nonExistingKey
  }

  "created" should "return the element in bag-info.txt with key 'Created' if present" in {
    val bag = simpleBagV0()
    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))

    inside(bag.created) {
      case Success(Some(dateTime)) =>
        dateTime.getMillis shouldBe expected.getMillis
    }
  }

  it should "return None if the key 'Created' is not present" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(DansV0Bag.CREATED_KEY)

    bag.created should matchPattern { case Success(None) => }
  }

  it should "fail when the key 'Created' contains something that is not a date/time" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(DansV0Bag.CREATED_KEY)
    bag.locBag.getMetadata.add(DansV0Bag.CREATED_KEY, "not-a-date")

    bag.created should matchPattern {
      case Failure(e: IllegalArgumentException) if e.getMessage == "Invalid format: \"not-a-date\"" =>
    }
  }

  "withCreated" should "set the 'Created' key in the bag" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.remove(DansV0Bag.CREATED_KEY)

    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))
    val resultBag = bag.withCreated(expected)

    inside(resultBag.created) {
      case Success(Some(dateTime)) => dateTime.getMillis shouldBe expected.getMillis
    }
  }

  it should "discard the former 'Created' key, such that there is always at most one value for this key" in {
    val bag = simpleBagV0()

    val expected = new DateTime(2017, 1, 16, 14, 35, 0, 888, DateTimeZone.forOffsetHoursMinutes(1, 0))
    val resultBag = bag.withCreated(expected)

    resultBag.locBag.getMetadata.get(DansV0Bag.CREATED_KEY).size() shouldBe 1

    inside(resultBag.created) {
      case Success(Some(dateTime)) => dateTime.getMillis shouldBe expected.getMillis
    }
  }

  "withoutCreated" should "remove the 'Created' key from the bag-info" in {
    val bag = simpleBagV0()

    bag.bagInfo should contain key DansV0Bag.CREATED_KEY

    val resultBag = bag.withoutCreated()
    resultBag.bagInfo shouldNot contain key DansV0Bag.CREATED_KEY
  }

  it should "not fail when the 'Created' key was not present in the bag-info" in {
    val bag = simpleBagV0()

    val resultBag = bag.withoutCreated()
    resultBag.bagInfo shouldNot contain key DansV0Bag.CREATED_KEY

    val resultBag2 = resultBag.withoutCreated()
    resultBag2.bagInfo shouldNot contain key DansV0Bag.CREATED_KEY
  }

  "isVersionOf" should "return the urn:uuid of the 'Is-Version-Of' key in the bag-info" in {
    val bag = fetchBagV0()
    val expected = new URI("urn:uuid:00000000-0000-0000-0000-000000000001")

    bag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  it should "return None when no 'Is-Version-Of' key is present in the bag-info" in {
    val bag = simpleBagV0()

    bag.isVersionOf should matchPattern { case Success(None) => }
  }

  it should "fail when the 'Is-Version-Of' key contains something else than a URI" in {
    val bag = simpleBagV0()
    bag.locBag.getMetadata.add(DansV0Bag.IS_VERSION_OF_KEY, "not-a-uri")

    bag.isVersionOf should matchPattern {
      case Failure(e: IllegalStateException) if e.getMessage == "Invalid format: \"not-a-uri\"" =>
    }
  }

  "withIsVersionOf" should "set the 'Is-Version-Of' key in the bag" in {
    val bag = simpleBagV0()

    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val expected = new URI(s"urn:uuid:$uuid")
    val resultBag = bag.withIsVersionOf(uuid)

    resultBag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  it should "discard the former 'Is-Version-Of' key, such that there is always at most one value for this key" in {
    val bag = fetchBagV0()

    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val expected = new URI(s"urn:uuid:$uuid")
    val resultBag = bag.withIsVersionOf(uuid)

    resultBag.locBag.getMetadata.get(DansV0Bag.IS_VERSION_OF_KEY).size() shouldBe 1

    resultBag.isVersionOf should matchPattern { case Success(Some(`expected`)) => }
  }

  "withoutIsVersionOf" should "remove the 'Is-Version-Of' key from the bag-info" in {
    val bag = fetchBagV0()

    bag.bagInfo should contain key DansV0Bag.IS_VERSION_OF_KEY

    val resultBag = bag.withoutIsVersionOf()
    resultBag.bagInfo shouldNot contain key DansV0Bag.IS_VERSION_OF_KEY
  }

  it should "not fail when the 'Is-Version-Of' key was not present in the bag-info" in {
    val bag = fetchBagV0()

    bag.bagInfo should contain key DansV0Bag.IS_VERSION_OF_KEY

    val resultBag = bag.withoutIsVersionOf()
    resultBag.bagInfo shouldNot contain key DansV0Bag.IS_VERSION_OF_KEY

    val resultBag2 = bag.withoutIsVersionOf()
    resultBag2.bagInfo shouldNot contain key DansV0Bag.IS_VERSION_OF_KEY
  }

  "easyUserAccount" should "return Success(Option.empty) if no account present" in {
    simpleBagV0().easyUserAccount shouldBe Success(Option.empty)
  }

  it should "return the one account if one is present" in {
    simpleBagV0().withEasyUserAccount("someAccount").easyUserAccount shouldBe Success(Some("someAccount"))
  }

  it should "return a Failure of IllegalStateException if more than one account is found" in {
    simpleBagV0()
      .addBagInfo(DansV0Bag.EASY_USER_ACCOUNT_KEY, "account1")
      .addBagInfo(DansV0Bag.EASY_USER_ACCOUNT_KEY, "account2").easyUserAccount should matchPattern {
      case Failure(e: IllegalStateException) if e.getMessage.startsWith("Only one EASY-User-Account allowed") =>

    }
  }

  "withEasyUserAccount" should "add EASY-User-Account to bag-info.txt" in {
    val bag = simpleBagV0()

    bag.bagInfo shouldNot contain key DansV0Bag.EASY_USER_ACCOUNT_KEY

    val resultBag = bag.withEasyUserAccount("someAccount")

    resultBag.bagInfo should contain key DansV0Bag.EASY_USER_ACCOUNT_KEY
    bag.bagInfo(DansV0Bag.EASY_USER_ACCOUNT_KEY) shouldBe Seq("someAccount")
  }

  it should "remove the old EASY-User-Account value from bag-info.txt" in {
    val bag = simpleBagV0().withEasyUserAccount("someAccount1")

    bag.bagInfo should contain key DansV0Bag.EASY_USER_ACCOUNT_KEY
    bag.bagInfo(DansV0Bag.EASY_USER_ACCOUNT_KEY) shouldBe Seq("someAccount1")

    val resultBag = bag.withEasyUserAccount("someAccount2")

    resultBag.bagInfo should contain key DansV0Bag.EASY_USER_ACCOUNT_KEY
    bag.bagInfo(DansV0Bag.EASY_USER_ACCOUNT_KEY) shouldBe Seq("someAccount2")
  }

  "withoutEasyUserAccount" should "remove EASY-User-Account to bag-info.txt" in {
    val bag = simpleBagV0().withEasyUserAccount("someAccount")

    bag.bagInfo should contain key DansV0Bag.EASY_USER_ACCOUNT_KEY

    val resultBag = bag.withoutEasyUserAccount()

    resultBag.bagInfo shouldNot contain key DansV0Bag.EASY_USER_ACCOUNT_KEY
  }
}
