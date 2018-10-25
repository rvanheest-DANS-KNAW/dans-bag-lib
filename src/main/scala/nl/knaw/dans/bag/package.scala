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
package nl.knaw.dans

import java.nio.file.Path

import better.files.File

import scala.language.implicitConversions

package object bag {

  implicit def betterFileToPath(file: File): Path = file.path

  object ImportOption extends Enumeration {

    type ImportOption = Value
    val COPY: ImportOption = Value

    /** Falls back on copy+delete if an ATOMIC_MOVE is not possible, for example due to different mounts.
     * In case of an interrupt a copy might have been completed without the delete.
     */
    val MOVE: ImportOption = Value

    /** Fails if source and target are on different mounts. */
    val ATOMIC_MOVE: ImportOption = Value
  }
}
