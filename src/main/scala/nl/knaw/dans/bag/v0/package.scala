package nl.knaw.dans.bag

import java.nio.file.Path

import better.files.File

import scala.language.implicitConversions

package object v0 {

  type RelativePath = File => File

  implicit def betterFileToPath(file: File): Path = file.path
}
