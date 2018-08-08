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

import nl.knaw.dans.bag.fixtures.{ TestBags, TestSupportFixture }

import scala.language.postfixOps

class PathAccessorsSpec extends TestSupportFixture with TestBags {

  "baseDir" should "return the root directory of the bag" in {
    simpleBagV0().baseDir shouldBe simpleBagDirV0
  }

  "data" should "point to the root of the bag/data directory" in {
    val bag = simpleBagV0()
    bag.data.toJava shouldBe (bag.baseDir / "data" toJava)
    bag.data.listRecursively.toList should contain only(
      bag.data / "x",
      bag.data / "y",
      bag.data / "z",
      bag.data / "sub",
      bag.data / "sub" / "u",
      bag.data / "sub" / "v",
      bag.data / "sub" / "w",
    )
  }
}
