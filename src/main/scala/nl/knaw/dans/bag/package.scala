package nl.knaw.dans

import java.nio.file.Path

import better.files.File

import scala.language.implicitConversions

package object bag {

  type RelativePath = File => File

  implicit def betterFileToPath(file: File): Path = file.path
}
