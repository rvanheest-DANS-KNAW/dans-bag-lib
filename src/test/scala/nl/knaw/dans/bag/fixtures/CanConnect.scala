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
package nl.knaw.dans.bag.fixtures

import java.net.{ HttpURLConnection, URL }

import org.scalatest.{ Matchers, Outcome, Retries, TestSuite }

import scala.util.Try

trait CanConnect extends TestSuite with Retries {
  this: Matchers =>

  override def withFixture(test: NoArgTest): Outcome = {
    if (isRetryable(test))
      withRetry { super.withFixture(test) }
    else
      super.withFixture(test)
  }

  def assumeCanConnect(urls: URL*): Unit = {
    assume(Try {
      urls.map(url => {
        url.openConnection match {
          case connection: HttpURLConnection =>
            connection.setConnectTimeout(5000)
            connection.setReadTimeout(5000)
            connection.connect()
            connection.disconnect()
            true
          case connection => throw new IllegalArgumentException(s"unknown connection type: $connection")
        }
      })
    }.isSuccess)
  }
}
