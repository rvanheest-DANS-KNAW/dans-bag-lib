package nl.knaw.dans.bag.v0

import java.io.InputStream
import java.net.{ HttpURLConnection, URI, URL, URLConnection }
import java.nio.charset.Charset
import java.nio.file.{ FileAlreadyExistsException, NoSuchFileException, Files => jFiles }
import java.util.{ UUID, Set => jSet }

import better.files.{ CloseableOps, Disposable, File, Files, ManagedResource }
import gov.loc.repository.bagit.creator.BagCreator
import gov.loc.repository.bagit.domain.{ Version, Bag => LocBag, Manifest => LocManifest, Metadata => LocMetadata }
import gov.loc.repository.bagit.reader.BagReader
import gov.loc.repository.bagit.util.PathUtils
import gov.loc.repository.bagit.writer.{ BagitFileWriter, FetchWriter, ManifestWriter, MetadataWriter }
import nl.knaw.dans.bag.ChecksumAlgorithm.{ ChecksumAlgorithm, locDeconverter }
import nl.knaw.dans.bag.{ ChecksumAlgorithm, FetchItem, DansBag, RelativePath, betterFileToPath }
import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

class DansV0Bag private(private[v0] val locBag: LocBag) extends DansBag {

  override def equals(obj: Any): Boolean = locBag.equals(obj)

  override def hashCode(): Int = locBag.hashCode()

  override def toString: String = locBag.toString

  /**
   * The base directory of the bag this object represents
   */
  override val baseDir: File = locBag.getRootDir

  /**
   * The data directory of the bag this object represents
   */
  override val data: File = baseDir / "data"

  /**
   * @inheritdoc
   */
  override def bagitVersion: Version = locBag.getVersion

  /**
   * @inheritdoc
   */
  override def withBagitVersion(version: Version): DansV0Bag = {
    // TODO what happens when the version changes? Should we change the layout of the bag?
    locBag.setVersion(version)
    this
  }

  /**
   * @inheritdoc
   */
  override def withBagitVersion(major: Int, minor: Int): DansV0Bag = {
    withBagitVersion(new Version(major, minor))
  }

  /**
   * @inheritdoc
   */
  override def fileEncoding: Charset = locBag.getFileEncoding

  /**
   * @inheritdoc
   */
  override def withFileEncoding(charset: Charset): DansV0Bag = {
    locBag.setFileEncoding(charset)
    this
  }

  /**
   * @inheritdoc
   */
  override def bagInfo: Map[String, Seq[String]] = {
    locBag.getMetadata.getAll.asScala
      .groupBy(_.getKey)
      .map { case (key, tuple) => key -> tuple.map(_.getValue) }
  }

  /**
   * @inheritdoc
   */
  override def addBagInfo(key: String, value: String): DansV0Bag = {
    locBag.getMetadata.add(key, value)

    this
  }

  /**
   * @inheritdoc
   */
  override def removeBagInfo(key: String): DansV0Bag = {
    locBag.getMetadata.remove(key)

    this
  }

  /**
   * @inheritdoc
   */
  override def created: Try[Option[DateTime]] = Try {
    Option(locBag.getMetadata.get(DansV0Bag.CREATED_KEY))
      .flatMap(_.asScala.headOption)
      .map(DateTime.parse(_, DansV0Bag.dateTimeFormatter))
  }

  /**
   * @inheritdoc
   */
  // TODO when should this be called? On creating new bag? On save (probably not)?
  override def withCreated(created: DateTime = DateTime.now()): DansV0Bag = {
    withoutCreated()
      .locBag.getMetadata.add(DansV0Bag.CREATED_KEY, created.toString(DansV0Bag.dateTimeFormatter))

    this
  }

  /**
   * @inheritdoc
   */
  override def withoutCreated(): DansV0Bag = {
    removeBagInfo(DansV0Bag.CREATED_KEY)
  }

  /**
   * @inheritdoc
   */
  override def isVersionOf: Try[Option[URI]] = {
    Option(locBag.getMetadata.get(DansV0Bag.IS_VERSION_OF_KEY))
      .flatMap(_.asScala.headOption)
      .map {
        case s if s matches "urn:uuid:[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}" =>
          Success(Option(new URI(s)))
        case s =>
          Failure(new IllegalArgumentException(s"""Invalid format: "$s""""))
      }
      .getOrElse(Success(Option.empty))
  }

  /**
   * @inheritdoc
   */
  override def withIsVersionOf(uuid: UUID): DansV0Bag = {
    val uri = new URI(s"urn:uuid:$uuid")
    withoutIsVersionOf()
      .locBag.getMetadata.add(DansV0Bag.IS_VERSION_OF_KEY, uri.toString)

    this
  }

  /**
   * @inheritdoc
   */
  override def withoutIsVersionOf(): DansV0Bag = {
    removeBagInfo(DansV0Bag.IS_VERSION_OF_KEY)
  }

  /**
   * @inheritdoc
   */
  override def fetchFiles: Seq[FetchItem] = locBag.getItemsToFetch.asScala.map(fetch => fetch: FetchItem)

  /**
   * @inheritdoc
   */
  override def addFetchItem(url: URL, pathInData: RelativePath): Try[DansV0Bag] = Try {
    val destinationPath = pathInData(data)
    var length: Long = 0L

    if (destinationPath.exists)
      throw new FileAlreadyExistsException(destinationPath.toString(), null, "already exists in payload")
    if (fetchFiles.find(_.file == destinationPath).isDefined)
      throw new FileAlreadyExistsException(destinationPath.toString(), null, "already exists in fetch.txt")
    if (!destinationPath.isChildOf(data))
      throw new IllegalArgumentException(s"a fetch file can only point to a location inside the bag/data directory; $destinationPath is outside the data directory")
    if (url.getProtocol != "http" && url.getProtocol != "https")
      throw new IllegalArgumentException("url can only have host 'http' or 'https'")

    downloadFetchFile(url)((input, dest) => {
      val tempDest = dest / destinationPath.name
      jFiles.copy(input, tempDest.path)
      require(tempDest.exists, s"copy from $url to $tempDest did not succeed")
      length = tempDest.size

      for (manifest <- locBag.getPayLoadManifests.asScala;
           algorithm: ChecksumAlgorithm = manifest.getAlgorithm)
        manifest.getFileToChecksumMap.put(destinationPath, tempDest.checksum(algorithm).toLowerCase)
    })

    if (locBag.getItemsToFetch.isEmpty) {
      val fetchFilePath = baseDir / "fetch.txt"
      for (tagmanifest <- locBag.getTagManifests.asScala;
           map = tagmanifest.getFileToChecksumMap)
        map.put(fetchFilePath, "***unknown, will be recomputed on calling '.save()'***")
    }

    locBag.getItemsToFetch.add(FetchItem(url, length, destinationPath))

    this
  }

  /**
   * @inheritdoc
   */
  override def removeFetchItem(pathInData: RelativePath): Try[DansV0Bag] = Try {
    val destinationPath = pathInData(data)

    fetchFiles.find(_.file == destinationPath)
      .map(removeFetchItem)
      .getOrElse { throw new NoSuchFileException(destinationPath.toString) }
  }

  /**
   * @inheritdoc
   */
  override def removeFetchItem(url: URL): Try[DansV0Bag] = Try {
    fetchFiles.find(_.url == url)
      .map(removeFetchItem)
      .getOrElse { throw new IllegalArgumentException(s"no such URL: $url") }
  }

  /**
   * @inheritdoc
   */
  override def removeFetchItem(item: FetchItem): DansV0Bag = {
    if (locBag.getItemsToFetch.remove(FetchItem.locDeconverter(item)))
      removeFileFromManifests(item.file, locBag.getPayLoadManifests, locBag.setPayLoadManifests)

    if (locBag.getItemsToFetch.isEmpty) {
      val fetchFilePath = baseDir / "fetch.txt"
      for (tagmanifest <- locBag.getTagManifests.asScala;
           map = tagmanifest.getFileToChecksumMap)
        map.remove(fetchFilePath.path)
    }

    this
  }

  /**
   * @inheritdoc
   */
  override def payloadManifestAlgorithms: Set[ChecksumAlgorithm] = algorithms(locBag.getPayLoadManifests)

  /**
   * @inheritdoc
   */
  override def addPayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                                           updateManifest: Boolean = false): Try[DansV0Bag] = Try {
    addAlgorithm(checksumAlgorithm, updateManifest, includeFetchFiles = true)(
      locBag.getPayLoadManifests, data.listRecursively.filter(_.isRegularFile), fetchFiles)

    val manifestPath = baseDir / s"manifest-${ checksumAlgorithm.getBagitName }.txt"
    for (tagmanifest <- locBag.getTagManifests.asScala;
         map = tagmanifest.getFileToChecksumMap)
      map.put(manifestPath, "***unknown, will be recomputed on calling '.save()'***")

    this
  }

  /**
   * @inheritdoc
   */
  override def removePayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[DansV0Bag] = Try {
    removeAlgorithm(checksumAlgorithm)(locBag.getPayLoadManifests, locBag.setPayLoadManifests)

    val manifestPath = baseDir / s"manifest-${ checksumAlgorithm.getBagitName }.txt"
    for (tagmanifest <- locBag.getTagManifests.asScala;
         map = tagmanifest.getFileToChecksumMap)
      map.remove(manifestPath.path)

    this
  }

  /**
   * @inheritdoc
   */
  override def tagManifestAlgorithms: Set[ChecksumAlgorithm] = algorithms(locBag.getTagManifests)

  /**
   * @inheritdoc
   */
  override def addTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                                       updateManifest: Boolean = false): Try[DansV0Bag] = Try {
    addAlgorithm(checksumAlgorithm, updateManifest)(locBag.getTagManifests,
      baseDir.listRecursively
        .filter(_.isRegularFile)
        .filterNot(_ isChildOf data)
        .filterNot(_.name startsWith "tagmanifest-"))

    this
  }

  /**
   * @inheritdoc
   */
  override def removeTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[DansV0Bag] = Try {
    removeAlgorithm(checksumAlgorithm)(locBag.getTagManifests, locBag.setTagManifests)

    this
  }

  /**
   * @inheritdoc
   */
  override def payloadManifests: Map[ChecksumAlgorithm, Map[File, String]] = manifests(locBag.getPayLoadManifests)

  /**
   * @inheritdoc
   */
  override def addPayloadFile(inputStream: InputStream)(pathInData: RelativePath): Try[DansV0Bag] = Try {
    val file = pathInData(data)

    if (file.exists)
      throw new FileAlreadyExistsException(file.toString)
    if (!data.isParentOf(file))
      throw new IllegalArgumentException(s"pathInData '$file' is supposed to point to a file that is a child of the bag/data directory")
    if (fetchFiles.map(_.file) contains file)
      throw new FileAlreadyExistsException(file.toString(), null, "file already present in bag as a fetch file")

    file.parent.createDirectories()

    addFile(inputStream, file, locBag.getPayLoadManifests)

    this
  }

  /**
   * @inheritdoc
   */
  override def addPayloadFile(src: File)(pathInData: RelativePath): Try[DansV0Bag] = Try {
    addFile(src, pathInData)(_.addPayloadFile)

    this
  }

  /**
   * @inheritdoc
   */
  override def removePayloadFile(pathInData: RelativePath): Try[DansV0Bag] = Try {
    val file = pathInData(data)

    if (file.notExists)
      throw new NoSuchFileException(file.toString)
    if (!data.isParentOf(file))
      throw new IllegalArgumentException(s"pathInData '$file' is supposed to point to a file that is a child of the bag/data directory")
    if (file.isDirectory)
      throw new IllegalArgumentException(s"cannot remove directory '$file'; you can only remove files")

    removeFile(file, data, locBag.getPayLoadManifests, locBag.setPayLoadManifests)

    this
  }

  /**
   * @inheritdoc
   */
  override def tagManifests: Map[ChecksumAlgorithm, Map[File, String]] = manifests(locBag.getTagManifests)

  /**
   * @inheritdoc
   */
  override def addTagFile(inputStream: InputStream)(pathInBag: RelativePath): Try[DansV0Bag] = Try {
    val file = pathInBag(baseDir)

    if (file.exists)
      throw new FileAlreadyExistsException(file.toString)
    if (data.isParentOf(file))
      throw new IllegalArgumentException(s"cannot add a tag file like '$file' to the bag/data directory")
    if (!baseDir.isParentOf(file))
      throw new IllegalArgumentException(s"cannot add a tag file like '$file' to a place outside the bag directory")
    if (file == baseDir / "bag-info.txt") {
      // you can only reach this point when you first remove the file manually
      throw new IllegalArgumentException("tag file 'bag-info.txt' is controlled by the library itself; you cannot add a file to this location")
    }
    if (file == baseDir / "bagit.txt") {
      // you can only reach this point when you first remove the file manually
      throw new IllegalArgumentException("tag file 'bagit.txt' is controlled by the library itself; you cannot add a file to this location")
    }
    if (file == baseDir / "fetch.txt") {
      // you can only reach this point when you first remove the file manually
      throw new IllegalArgumentException("tag file 'fetch.txt' is controlled by the library itself; you cannot add a file to this location")
    }
    if (file.parent == baseDir && file.name.startsWith("manifest-") && file.name.endsWith(".txt")) {
      // you can only reach this point when you first remove the file manually
      throw new IllegalArgumentException("manifest files are controlled by the library itself; you cannot add a file to this location")
    }
    if (file.parent == baseDir && file.name.startsWith("tagmanifest-") && file.name.endsWith(".txt")) {
      // you can only reach this point when you first remove the file manually
      throw new IllegalArgumentException("tagmanifest files are controlled by the library itself; you cannot add a file to this location")
    }

    // highest possible parent is the bag directory, which already exists,
    // in which case no directories are created
    file.parent.createDirectories()

    addFile(inputStream, file, locBag.getTagManifests)

    this
  }

  /**
   * @inheritdoc
   */
  override def addTagFile(src: File)(pathInBag: RelativePath): Try[DansV0Bag] = Try {
    addFile(src, pathInBag)(_.addTagFile)

    this
  }

  /**
   * @inheritdoc
   */
  override def removeTagFile(pathInBag: RelativePath): Try[DansV0Bag] = Try {
    val file = pathInBag(baseDir)

    if (file.notExists)
      throw new NoSuchFileException(file.toString)
    if (file == baseDir)
      throw new IllegalArgumentException("cannot remove the whole bag")
    if (file.isDirectory)
      throw new IllegalArgumentException(s"cannot remove directory '$file'; you can only remove files")
    if (file.isChildOf(data))
      throw new IllegalArgumentException(s"cannot remove '$file' since it is a child of the bag/data directory")
    if (!file.isChildOf(baseDir))
      throw new IllegalArgumentException(s"cannot remove '$file' since it is not a child of the bag directory")
    if (file == baseDir / "bag-info.txt" || file == baseDir / "bagit.txt" || file == baseDir / "fetch.txt")
      throw new IllegalArgumentException(s"cannot remove bag specific file '$file'")
    if (file.parent == baseDir && (file.name.startsWith("manifest-") && file.name.endsWith(".txt")))
      throw new IllegalArgumentException(s"cannot remove manifest file '$file'")
    if (file.parent == baseDir && (file.name.startsWith("tagmanifest-") && file.name.endsWith(".txt")))
      throw new IllegalArgumentException(s"cannot remove tagmanifest file '$file'")

    removeFile(file, baseDir, locBag.getTagManifests, locBag.setTagManifests)

    this
  }

  /**
   * @inheritdoc
   */
  override def save(): Try[Unit] = Try {
    if (payloadManifestAlgorithms.isEmpty)
      throw new IllegalStateException("bag must contain at least one payload manifest")
    if (!baseDir.isWriteable)
      throw new IllegalStateException(s"bag located in '$baseDir' is not writeable")

    // save bagit.txt file
    BagitFileWriter.writeBagitFile(bagitVersion, fileEncoding, baseDir)

    // save manifest-<alg>.txt files
    // first delete all manifest files:
    //   1. manifest files that are no longer needed are being deleted here
    //   2. in LoC-BagitJava the lines are being appended to the existing manifests,
    //      which will result in duplications. Deleting the files first will avoid that.
    for (file <- this.glob("manifest-*.txt"))
      file.delete()
    ManifestWriter.writePayloadManifests(locBag.getPayLoadManifests, baseDir, baseDir, fileEncoding)

    // save bag-info.txt file
    locBag.getMetadata.upsertPayloadOxum(PathUtils.generatePayloadOxum(data))
    locBag.getMetadata.remove("Bagging-Date") //remove the old bagging date if it exists so that there is only one
    locBag.getMetadata.add("Bagging-Date", DateTime.now().toString(ISODateTimeFormat.yearMonthDay()))
    // TODO calculate correct Bag-Size property
    MetadataWriter.writeBagMetadata(locBag.getMetadata, bagitVersion, baseDir, fileEncoding)

    // save fetch.txt file
    val fetchTxt = baseDir / "fetch.txt"
    if (fetchTxt.exists) fetchTxt.delete()
    if (!locBag.getItemsToFetch.isEmpty)
      FetchWriter.writeFetchFile(locBag.getItemsToFetch, baseDir, baseDir, fileEncoding)

    // calculate and save tagmanifest-<alg>.txt files
    for (tagmanifest <- locBag.getTagManifests.asScala;
         algorithm: ChecksumAlgorithm = tagmanifest.getAlgorithm;
         path <- tagmanifest.getFileToChecksumMap.keySet().asScala;
         file: File = path) {
      tagmanifest.getFileToChecksumMap.compute(file, (_, _) => file.checksum(algorithm).toLowerCase)
    }
    // delete all tagmanifest files:
    //   1. tagmanifest files that are no longer needed are being deleted here
    //   2. in LoC-BagitJava the lines are being appended to the existing tagmanifests,
    //      which will result in duplications. Deleting the files first will avoid that.
    for (file <- this.glob("tagmanifest-*.txt"))
      file.delete()
    ManifestWriter.writeTagManifests(locBag.getTagManifests, baseDir, baseDir, fileEncoding)
  }

  protected def openConnection(url: URL): ManagedResource[URLConnection] = {
    url.openConnection() match {
      case conn: HttpURLConnection =>
        conn.setConnectTimeout(1000)
        conn.setReadTimeout(1000)

        new ManagedResource(conn)
      case _ =>
        throw new IllegalArgumentException("only 'http' and 'https' urls accepted")
    }
  }

  implicit protected def disposeURLConnection: Disposable[URLConnection] = {
    Disposable(_ match {
      case conn: HttpURLConnection => conn.disconnect()
      case _ => // nothing to close
    })
  }

  private def downloadFetchFile[U](url: URL)(doAfterDownload: (InputStream, File) => U): Unit = {
    for (conn <- openConnection(url);
         _ = conn.connect();
         input <- url.openStream().autoClosed;
         dest <- File.temporaryDirectory(prefix = "fetch-file", parent = Option(baseDir).filter(_.isWriteable)))
      doAfterDownload(input, dest)
  }

  private def algorithms(ms: jSet[LocManifest]): Set[ChecksumAlgorithm] = {
    ms.asScala.map(_.getAlgorithm: ChecksumAlgorithm).toSet
  }

  private def addAlgorithm(checksumAlgorithm: ChecksumAlgorithm, updateManifest: Boolean = false,
                           includeFetchFiles: Boolean = false)
                          (getManifests: jSet[LocManifest], payload: Files,
                           fetchItems: => Seq[FetchItem] = Seq.empty): Unit = {
    val (fileToChecksumMap, isNew) = getManifests.asScala
      .collectFirst {
        case m if (m.getAlgorithm: ChecksumAlgorithm) == checksumAlgorithm =>
          (m.getFileToChecksumMap, false)
      }
      .getOrElse {
        val manifest = new LocManifest(checksumAlgorithm)
        getManifests.add(manifest)
        (manifest.getFileToChecksumMap, true)
      }

    if (isNew || updateManifest) {
      for (file <- payload)
        fileToChecksumMap.put(file, file.checksum(checksumAlgorithm).toLowerCase)
    }

    if (includeFetchFiles) {
      for (FetchItem(url, _, fetchFile) <- fetchItems) {
        downloadFetchFile(url)((input, dest) => {
          val tempDest = dest / fetchFile.name
          jFiles.copy(input, tempDest.path)
          require(tempDest.exists, s"copy from $url to $tempDest did not succeed")

          fileToChecksumMap.put(fetchFile, tempDest.checksum(checksumAlgorithm).toLowerCase)
        })
      }
    }
  }

  private def removeAlgorithm(checksumAlgorithm: ChecksumAlgorithm)
                             (getManifests: jSet[LocManifest],
                              setManifests: jSet[LocManifest] => Unit): Unit = {
    setManifests {
      val remainingAlgorithms = getManifests.asScala.toList
        .filterNot(manifest => (manifest.getAlgorithm: ChecksumAlgorithm) == checksumAlgorithm)

      if (remainingAlgorithms.size == getManifests.size)
        throw new NoSuchElementException(s"No manifest found for checksum $checksumAlgorithm")

      remainingAlgorithms.toSet.asJava
    }
  }

  private def manifests(ms: jSet[LocManifest]): Map[ChecksumAlgorithm, Map[File, String]] = {
    ms.asScala
      .map(manifest => (manifest.getAlgorithm: ChecksumAlgorithm) ->
        manifest.getFileToChecksumMap.asScala.toMap.map {
          case (path, checksum) => File(path) -> checksum
        })
      .toMap
  }

  private def addFile(inputStream: InputStream, dest: File, manifests: jSet[LocManifest]): Unit = {
    jFiles.copy(inputStream, dest)

    for (manifest <- manifests.asScala;
         algorithm: ChecksumAlgorithm = manifest.getAlgorithm)
      manifest.getFileToChecksumMap.put(dest, dest.checksum(algorithm).toLowerCase)
  }

  private def addFile(src: File, pathInBag: RelativePath)
                     (addFileAsStream: DansV0Bag => InputStream => RelativePath => Try[DansV0Bag]): Unit = {

    def calculatePathInBagToFile(file: File)(pathInBagToFile: RelativePath): RelativePath = {
      pathInBagToFile(_) / file.name
    }

    @tailrec
    def recursion(bag: DansV0Bag, currentFile: File, pathInBagToFile: RelativePath)
                 (implicit backlog: mutable.Queue[(File, RelativePath)]): DansV0Bag = {
      if (currentFile.isDirectory) {
        val subFiles = currentFile.list
          .map(file => file -> calculatePathInBagToFile(file)(pathInBagToFile))
          .toList
        backlog.enqueue(subFiles: _*)

        if (backlog.isEmpty)
          bag // end of recursion, backtrack
        else {
          val (nextFile, pathInBagToNextFile) = backlog.dequeue()
          recursion(bag, nextFile, pathInBagToNextFile)
        }
      }
      else {
        assert(currentFile.isRegularFile, s"$currentFile is supposed to be a regular file")

        currentFile.inputStream()(in => addFileAsStream(bag)(in)(pathInBagToFile)) match {
          case Success(resultBag) if backlog.isEmpty => resultBag // end of recursion, backtrack
          case Success(resultBag) =>
            val (nextFile, pathInBagToNextFile) = backlog.dequeue()
            recursion(resultBag, nextFile, pathInBagToNextFile)
          case Failure(e) => throw e // get out of recursion completely
        }
      }
    }

    recursion(this, src, pathInBag)(mutable.Queue.empty)
  }

  private def removeFile(file: File, haltDeleteAt: File, manifests: jSet[LocManifest],
                         setManifests: jSet[LocManifest] => Unit): Unit = {
    file.delete()
    recursiveClean(file.parent)

    @tailrec
    def recursiveClean(file: File): Unit = {
      if (file != haltDeleteAt && file.isDirectory && file.isChildOf(haltDeleteAt) && file.children.size < 1) {
        file.delete()
        recursiveClean(file.parent)
      }
    }

    removeFileFromManifests(file, manifests, setManifests)
  }

  private def removeFileFromManifests(file: File, manifests: jSet[LocManifest],
                                      setManifests: jSet[LocManifest] => Unit): Unit = {
    for (manifest <- manifests.asScala;
         fileChecksumMap = manifest.getFileToChecksumMap)
      fileChecksumMap.remove(file.path)

    setManifests {
      manifests.asScala.toList
        .filterNot(_.getFileToChecksumMap.isEmpty)
        .toSet.asJava
    }
  }
}

object DansV0Bag {
  private val bagReader = new BagReader()

  val CREATED_KEY = "Created"
  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()
  val IS_VERSION_OF_KEY = "Is-Version-Of"

  /**
   * Create an empty bag at the given `baseDir`. Based on the given `algorithms`, (empty)
   * `manifest-&lt;alg&gt;.txt` and (non-empty) `tagmanifest-&lt;alg&gt;.txt` files are created.
   * The `tagmanifest-&lt;alg&gt;.txt` contain the checksums for `bagit.txt`, `bag-info.txt` and
   * all manifest files.
   * The resulting bag also a `bag-info.txt` file with `Payload-Oxum`, `Bagging-Date` and all
   * given `bagInfo` key-value(s) pairs.
   *
   * There are no restrictions on the amount of `bagInfo` entries, apart from the limitations listed
   * in the `https://tools.ietf.org/html/draft-kunze-bagit-16 BagIt Spec`.
   * There must be at least one algorithm given for the checksums.
   *
   * @param baseDir    The directory in which the bag is created
   * @param algorithms The algorithms with which the checksums for the (payload/tag) files are calculated
   * @param bagInfo    The entries to be added to `bag-info.txt`
   * @return if successful, returns a `nl.knaw.dans.bag.v0.Bag` object representing the bag located at `baseDir`
   *         else returns an exception
   */
  def empty(baseDir: File,
            algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
            bagInfo: Map[String, Seq[String]] = Map.empty): Try[DansV0Bag] = {
    if (baseDir.exists)
      Failure(new FileAlreadyExistsException(baseDir.toString))
    else
      bagInPlace(baseDir.createDirectories(), algorithms, bagInfo)
  }

  /**
   * Create a bag based on the payload that is inside `payloadDir`. The payload is moved to a
   * subdirectory `data/` and from there the bag is created around it. Based on the given
   * `algorithms`, `manifest-&lt;alg&gt;.txt` and `tagmanifest-&lt;alg&gt;.txt` files are created.
   * The `manifest-&lt;alg&gt;.txt` contain the checksums of the payload files in `data/`.
   * The `tagmanifest-&lt;alg&gt;.txt` contain the checksums for `bagit.txt`, `bag-info.txt` and
   * all manifest files.
   *
   * The resulting bag also a `bag-info.txt` file with `Payload-Oxum`, `Bagging-Date` and all
   * given `bagInfo` key-value(s) pairs.
   *
   * There are no restrictions on the amount of `bagInfo` entries, apart from the limitations listed
   * in the `https://tools.ietf.org/html/draft-kunze-bagit-16 BagIt Spec`.
   * There must be at least one algorithm given for the checksums.
   *
   * @param payloadDir The directory containing the payload files and in which the bag is created
   * @param algorithms The algorithms with which the checksums for the (payload/tag) files are calculated
   * @param bagInfo    The entries to be added to `bag-info.txt`
   * @return if successful, returns a `nl.knaw.dans.bag.v0.Bag` object representing the bag located at `payloadDir`
   *         else returns an exception
   */
  def createFromData(payloadDir: File,
                     algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
                     bagInfo: Map[String, Seq[String]] = Map.empty): Try[DansV0Bag] = {
    if (payloadDir.notExists)
      Failure(new NoSuchFileException(payloadDir.toString))
    else
      bagInPlace(payloadDir, algorithms, bagInfo)
  }

  private def bagInPlace(base: File,
                         algorithms: Set[ChecksumAlgorithm],
                         bagInfo: Map[String, Seq[String]]): Try[DansV0Bag] = Try {
    require(algorithms.nonEmpty, "At least one algorithm should be provided")

    val algos = algorithms.map(locDeconverter).asJava
    val metadata = new LocMetadata() {
      for ((key, values) <- bagInfo;
           value <- values) {
        add(key, value)
      }
    }
    new DansV0Bag(BagCreator.bagInPlace(base, algos, true, metadata))
  }

  /**
   * Reads a bag located at `baseDir` and return a `nl.knaw.dans.bag.v0.Bag` when successful.
   *
   * @param baseDir The directory containing the bag
   * @return if successful, returns a `nl.knaw.dans.bag.v0.Bag` object representing the bag located at `baseDir`
   *         else return an exception
   */
  def read(baseDir: File): Try[DansV0Bag] = Try {
    new DansV0Bag(bagReader.read(baseDir))
  }
}
