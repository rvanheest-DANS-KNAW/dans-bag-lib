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

trait Lipsum {

  private val lipsum1: String = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
    "Integer bibendum, nisi ac posuere luctus, nunc ante condimentum augue, eu fringilla lectus " +
    "risus at quam. Sed porta lacus non odio convallis, a semper eros porta. Curabitur arcu " +
    "ligula, feugiat non ligula nec, feugiat laoreet purus. Aliquam erat volutpat. Etiam neque " +
    "tortor, accumsan a risus ac, maximus molestie mi. Vivamus tellus ex, eleifend in erat eget, " +
    "hendrerit tincidunt erat. Aenean dictum blandit metus nec faucibus."

  private val lipsum2: String = "Suspendisse fringilla ex fermentum rutrum posuere. Praesent " +
    "scelerisque, felis nec viverra luctus, neque arcu fringilla arcu, sit amet congue tortor " +
    "neque eu nibh. Mauris erat neque, porttitor eget porttitor nec, tincidunt sed magna. " +
    "Quisque eu massa rhoncus, rutrum ante sed, pharetra mi. In id malesuada odio, in mollis " +
    "est. Morbi non nibh vel nisi viverra aliquam id vel lacus. Donec eu ornare lorem. " +
    "Suspendisse egestas metus eget ullamcorper imperdiet. Aenean volutpat in odio sit amet " +
    "convallis."

  private val lipsum3: String = "Sed nulla felis, malesuada eu tristique vel, mollis a mi. In " +
    "nec velit accumsan, blandit ante at, tincidunt odio. Curabitur blandit, velit et ornare " +
    "iaculis, lorem ipsum malesuada purus, quis fermentum urna leo sit amet ipsum. Integer nec " +
    "tortor semper sem dictum convallis quis a diam. Lorem ipsum dolor sit amet, consectetur " +
    "adipiscing elit. Pellentesque maximus facilisis accumsan. Vivamus felis elit, pulvinar sed " +
    "lectus sodales, iaculis gravida urna. Ut luctus ligula mauris, id condimentum neque " +
    "lobortis in. Integer volutpat tincidunt est, interdum dignissim tortor tempor at. " +
    "Suspendisse urna diam, sagittis nec egestas eu, mattis sed justo."

  private val lipsum4: String = "Cras laoreet fermentum dui id cursus. Etiam blandit lacinia " +
    "condimentum. Nam mollis nec massa pharetra blandit. Aliquam tempus molestie sapien, at " +
    "varius augue ultricies et. Vivamus vestibulum rhoncus libero, eu sagittis nisl mattis " +
    "eget. Vestibulum lectus diam, molestie in augue quis, condimentum blandit est. Nulla " +
    "pharetra auctor diam, a elementum dui laoreet et. Curabitur efficitur metus magna, vel " +
    "auctor velit congue eget. In at rutrum ligula."

  private val lipsum5: String = "Vestibulum ante ipsum primis in faucibus orci luctus et " +
    "ultrices posuere cubilia Curae; Nullam non finibus enim. Vivamus lacinia sed urna sed " +
    "rhoncus. Quisque tempus pharetra semper. Sed rutrum lectus nibh, imperdiet ultrices " +
    "ligula sodales in. Vivamus mattis blandit sodales. Nam varius nisi leo, id porta dolor " +
    "mattis a. Curabitur sed magna purus. Ut egestas nisi non cursus tincidunt. Donec volutpat " +
    "eget nunc eget faucibus."

  private val lipsum = lipsum1 :: lipsum2 :: lipsum3 :: lipsum4 :: lipsum5 :: Nil

  def lipsum(n: Int): String = {
    lipsum.take(0 max n min 5).mkString("\n\n")
  }
}
