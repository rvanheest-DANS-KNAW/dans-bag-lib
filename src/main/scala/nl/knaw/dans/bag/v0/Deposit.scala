package nl.knaw.dans.bag.v0

import better.files.File
import nl.knaw.dans.bag.v0.Deposit._

import scala.util.{ Failure, Success, Try }

case class Deposit(baseDir: File, bag: Bag, properties: DepositProperties) {

  def write(): Try[Unit] = {
    for {
      _ <- bag.save()
      _ <- properties.write(baseDir / depositPropertiesName)
    } yield ()
  }
}
object Deposit {

  private val depositPropertiesName = "deposit.properties"

  def fromFile(baseDir: File): Try[Deposit] = {
    for {
      bagDir <- findBagDir(baseDir)
      bag <- Bag.read(bagDir)
      properties <- DepositProperties.fromFile(depositProperties(baseDir))
    } yield Deposit(baseDir, bag, properties)
  }

  private def findBagDir(baseDir: File): Try[File] = {
    baseDir.list.filter(_.isDirectory).toList match {
      case dir :: Nil => Success(dir)
      case Nil => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains no directories"))
      case _ => Failure(new IllegalArgumentException(s"$baseDir is not a deposit: it contains multiple directories"))
    }
  }

  private def depositProperties(baseDir: File): File = baseDir / depositPropertiesName
}
