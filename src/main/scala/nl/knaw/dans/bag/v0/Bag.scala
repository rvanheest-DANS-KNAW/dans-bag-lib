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
import nl.knaw.dans.bag.v0.ChecksumAlgorithm.{ ChecksumAlgorithm, locDeconverter }
import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }

class Bag private(private[v0] val locBag: LocBag) {

  override def equals(obj: scala.Any): Boolean = locBag.equals(obj)

  override def hashCode(): Int = locBag.hashCode()

  override def toString: String = locBag.toString

  /**
   * The base directory of the bag this object represents
   */
  val baseDir: File = locBag.getRootDir

  /**
   * The data directory of the bag this object represents
   */
  val data: File = baseDir / "data"

  /**
   * @return the bag's `gov.loc.repository.bagit.domain.Version`, which is stored in `bagit.txt`
   */
  def bagitVersion: Version = locBag.getVersion

  /**
   * Change the bag's `gov.loc.repository.bagit.domain.Version` to a new `Version`.
   * Please note that this will only be synced with `bagit.txt` once `Bag.save` is called.
   *
   * @param version the new `Version` of the bag
   * @return this bag, with the new `Version`
   */
  def withBagitVersion(version: Version): Bag = {
    // TODO what happens when the version changes? Should we change the layout of the bag?
    locBag.setVersion(version)
    this
  }

  /**
   * Change the bag's `gov.loc.repository.bagit.domain.Version` to a new `Version`.
   * Please note that this will only be synced with `bagit.txt` once `Bag.save` is called.
   *
   * @param major major part of the version number
   * @param minor minor part of the version number
   * @return this bag, with a new `Version` comprised of the `major` and `minor` inputs
   */
  def withBagitVersion(major: Int, minor: Int): Bag = {
    withBagitVersion(new Version(major, minor))
  }

  /**
   * @return the bag's `java.nio.charset.Charset`, which is stored in `bagit.txt`
   */
  def fileEncoding: Charset = locBag.getFileEncoding

  /**
   * Change the bag's `java.nio.charset.Charset` to a new `Charset`.
   * Please note that this will only be synced with `bagit.txt` once `Bag.save` is called.
   *
   * @param charset the new `Charset` of the bag
   * @return this bag, with the new `Charset`
   */
  def withFileEncoding(charset: Charset): Bag = {
    // TODO do we actually need to change the tag files to have a new/updated encoding?
    locBag.setFileEncoding(charset)
    this
  }

  /**
   * List all entries of `bag/bag-info.txt`. Key-value pairs will be grouped on key, such that if
   * any key is listed multiple times, their values will end up together in a `Seq[String]`.
   *
   * @return a Map containing all entries of `bag/bag-info.txt`
   */
  def bagInfo: Map[String, Seq[String]] = {
    locBag.getMetadata.getAll.asScala
      .groupBy(_.getKey)
      .map { case (key, tuple) => key -> tuple.map(_.getValue) }
  }

  /**
   * Add an entry to the bagInfo. Please note that this will only be synced with `bag-info.txt`
   * once `Bag.save` is called.
   *
   * @param key   the key of the new entry
   * @param value the value of the new entry
   * @return this bag, with the new entry added
   */
  def addBagInfo(key: String, value: String): Bag = {
    locBag.getMetadata.add(key, value)

    this
  }

  /**
   * Remove all entries with the given key. Please note that this will only be synced with
   * `bag-info.txt` once `Bag.save` is called.
   *
   * @param key the key to be removed
   * @return this bag, without the removed entries
   */
  def removeBagInfo(key: String): Bag = {
    locBag.getMetadata.remove(key)

    this
  }

  /**
   * Retrieves the value for key 'Created' from `bag-info.txt` as a parsed `org.joda.time.DateTime`
   * object. If this key has an invalid value, a `Failure` is returned instead. If the key is not
   * present, a `Success(None)` is returned instead.
   *
   * @return the `DateTime` object found in the 'Created' key in `bag-info.txt`
   */
  def created: Try[Option[DateTime]] = Try {
    Option(locBag.getMetadata.get(Bag.CREATED_KEY))
      .flatMap(_.asScala.headOption)
      .map(DateTime.parse(_, Bag.dateTimeFormatter))
  }

  /**
   * Adds the key 'Created' from `bag-info.txt`. If the key is already present, it is replaced with
   * the new value. When no argument is supplied, a default value `DateTime.now()` is used instead.
   * Please note that this will only be synced with `bag-info.txt` once `Bag.save` is called.
   *
   * @param created the `DateTime` object on which the bag's content was created
   * @return this bag, with the new value for 'Created' in `bag-info.txt`
   */
  // TODO when should this be called? On creating new bag? On save (probably not)?
  def withCreated(created: DateTime = DateTime.now()): Bag = {
    withoutCreated()
      .locBag.getMetadata.add(Bag.CREATED_KEY, created.toString(Bag.dateTimeFormatter))

    this
  }

  /**
   * Remove the entry with key 'Created' from `bag-info.txt`.
   * Please note that this will only be synced with `bag-info.txt` once `Bag.save` is called.
   *
   * @return this bag, without the removed entry
   */
  def withoutCreated(): Bag = {
    removeBagInfo(Bag.CREATED_KEY)
  }

  /**
   * Retrieves the value for key 'Is-Version-Of' from `bag-info.txt` as a `java.net.URI` object.
   * If this key has an invalid value (if it does not match the pattern
   * `urn:uuid:[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}`), a `Failure` is returned instead.
   * If the key is not present, a `Success(None)` is returned instead.
   *
   * @return the `URI` object found in the 'Is-Version-Of' key in `bag-info.txt`
   */
  def isVersionOf: Try[Option[URI]] = {
    Option(locBag.getMetadata.get(Bag.IS_VERSION_OF_KEY))
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
   * Adds the key 'Is-Version-Of' from `bag-info.txt`. If the key is already present, it is replaced
   * with the new value. The given `java.util.UUID` is used to construct a `java.net.URI` of the
   * form `urn:uuid:[uuid]`.
   * Please note that this will only be synced with `bag-info.txt` once `Bag.save` is called.
   *
   * @param uuid the `UUID` of the previous revision of this bag
   * @return this bag, with the new value for 'Is-Version-Of' in `bag-info.txt`
   */
  def withIsVersionOf(uuid: UUID): Bag = {
    val uri = new URI(s"urn:uuid:$uuid")
    withoutIsVersionOf()
      .locBag.getMetadata.add(Bag.IS_VERSION_OF_KEY, uri.toString)

    this
  }

  /**
   * Remove the entry with key 'Is-Version-Of' from `bag-info.txt`.
   * Please note that this will only be synced with `bag-info.txt` once `Bag.save` is called.
   *
   * @return this bag, without the removed entry
   */
  def withoutIsVersionOf(): Bag = {
    removeBagInfo(Bag.IS_VERSION_OF_KEY)
  }

  /**
   * Lists all files that are stated in `fetch.txt`.
   *
   * @return all entries listed in `fetch.txt`
   */
  def fetchFiles: Seq[FetchItem] = locBag.getItemsToFetch.asScala.map(fetch => fetch: FetchItem)

  // TODO document
  // TODO this may cause latency, due to downloading of file. Wrapping in a Promise/Observable is recommended!
  def addFetchFile(url: URL, length: Long, pathInData: RelativePath): Try[Bag] = Try {
    val destinationPath = pathInData(data)

    if (destinationPath.exists)
      throw new FileAlreadyExistsException(
        destinationPath.toString())
    if (!destinationPath.isChildOf(data))
      throw new IllegalArgumentException(s"a fetch file can only point to a location inside the bag/data directory; $destinationPath is outside the data directory")
    if (url.getProtocol != "http" && url.getProtocol != "https")
      throw new IllegalArgumentException("url can only have host 'http' or 'https'")

    downloadFetchFile(url)((input, dest) => {
      val tempDest = dest / destinationPath.name
      jFiles.copy(input, tempDest.path)
      require(tempDest.exists, s"copy from $url to $tempDest did not succeed")

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

  // TODO document
  def removeFetchByFile(pathInData: RelativePath): Try[Bag] = Try {
    val destinationPath = pathInData(data)

    fetchFiles.find(_.file == destinationPath)
      .map(removeFetch)
      .getOrElse { throw new NoSuchFileException(destinationPath.toString) }
  }

  // TODO document
  def removeFetchByURL(url: URL): Try[Bag] = Try {
    fetchFiles.find(_.url == url)
      .map(removeFetch)
      .getOrElse { throw new IllegalArgumentException(s"no such URL: $url") }
  }

  // TODO document
  def removeFetch(item: FetchItem): Bag = {
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
   * List all algorithms that are being used in this bag to calculate the checksums of the payload files.
   *
   * @return the set of algorithms used for calculating the payload's checksums
   */
  def payloadManifestAlgorithms: Set[ChecksumAlgorithm] = algorithms(locBag.getPayLoadManifests)

  /**
   * Add an algorithm to the bag that will be used for calculating the checksums of the payload files.
   * This method can also be used to update the checksums of payload files in case some files were
   * mutated by other means than the use of this library. Supply `updateManifest = true`
   * (default false) for this behaviour.
   * In both use cases of this method, the payload manifest will be added to all tag manifests with
   * a temporary value, indicating that the real checksum for the payload manifest will only be
   * recalculated on calling `Bag.save`.
   *
   * Please note that this new/updated algorithm will only be added to the bag on the file system
   * once `Bag.save` is called.
   *
   * @param checksumAlgorithm the algorithm for which the payload checksums will be added or updated
   * @param updateManifest    indicates whether it should update (`true`) or add (`false`) the algorithm
   * @return this bag, with the new algorithm added
   */
  // TODO add to documentation that checksums for fetch files are calculated as well
  // this may cause some latency. Wrapping in a Promise/Observable is recommended!
  def addPayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                                  updateManifest: Boolean = false): Try[Bag] = Try {
    addAlgorithm(checksumAlgorithm, updateManifest, includeFetchFiles = true)(
      locBag.getPayLoadManifests, data.listRecursively.filter(_.isRegularFile), fetchFiles)

    val manifestPath = baseDir / s"manifest-${ checksumAlgorithm.getBagitName }.txt"
    for (tagmanifest <- locBag.getTagManifests.asScala;
         map = tagmanifest.getFileToChecksumMap)
      map.put(manifestPath, "***unknown, will be recomputed on calling '.save()'***")

    this
  }

  /**
   * Remove an algorithm from the bag, which was used for calculating the checksums of the payload files.
   * It also removes the payload manifest from all tagmanifests as well.
   *
   * Please note that this change will only be applied to the bag on the file system once
   * `Bag.save` is called.
   *
   * @param checksumAlgorithm the algorithm to be removed from the bag with respect to the payload files
   * @return this bag, without the removed algorithm
   */
  def removePayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[Bag] = Try {
    removeAlgorithm(checksumAlgorithm)(locBag.getPayLoadManifests, locBag.setPayLoadManifests)

    val manifestPath = baseDir / s"manifest-${ checksumAlgorithm.getBagitName }.txt"
    for (tagmanifest <- locBag.getTagManifests.asScala;
         map = tagmanifest.getFileToChecksumMap)
      map.remove(manifestPath.path)

    this
  }

  /**
   * List all algorithms that are being used in this bag to calculate the checksums of the tag files.
   *
   * @return the set of algorithms used for calculating the tagfiles' checksums
   */
  def tagManifestAlgorithms: Set[ChecksumAlgorithm] = algorithms(locBag.getTagManifests)

  /**
   * Add an algorithm to the bag that will be used for calculating the checksums of the tag files.
   * This method can also be used to update the checksums of tag files in case some files were
   * mutated by other means than the use of this library. Supply `updateManifest = true`
   * (default false) for this behaviour.
   *
   * Please note that this new/updated algorithm will only be added to the bag on the file system
   * once `Bag.save` is called.
   *
   * @param checksumAlgorithm the algorithm for which the tagfiles' checksums will be added or updated
   * @param updateManifest    indicates whether it should update (`true`) or add (`false`) the algorithm
   * @return this bag, with the new algorithm added
   */
  def addTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                              updateManifest: Boolean = false): Try[Bag] = Try {
    addAlgorithm(checksumAlgorithm, updateManifest)(locBag.getTagManifests,
      baseDir.listRecursively
        .filter(_.isRegularFile)
        .filterNot(_ isChildOf data)
        .filterNot(_.name startsWith "tagmanifest-"))

    this
  }

  /**
   * Remove an algorithm from the bag, which was used for calculating the checksums of the tag files.
   *
   * Please note that this change will only be applied to the bag on the file system once
   * `Bag.save` is called.
   *
   * @param checksumAlgorithm the algorithm to be removed from the bag with respect to the tag files
   * @return this bag, without the removed algorithm
   */
  def removeTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[Bag] = Try {
    removeAlgorithm(checksumAlgorithm)(locBag.getTagManifests, locBag.setTagManifests)

    this
  }

  /**
   * For each algorithm, list the mappings of payload file to its checksum.
   *
   * @return the mapping of payload file to its checksum, for each algorithm
   */
  def payloadManifests: Map[ChecksumAlgorithm, Map[File, String]] = manifests(locBag.getPayLoadManifests)

  /**
   * Add a payload file from an `java.io.InputStream` to the bag at the position indicated by the
   * path relative to the `bag/data` directory. If the resolved destination of this new file already
   * exists within the bag, or if the resolved destination is outside of the `bag/data` directory,
   * this method will return a `scala.util.Failure`. This method also adds the checksum of the new
   * file to all payload manifests.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param inputStream the source of the new file to be added to the bag
   * @param pathInData  the path relative to the `bag/data` directory where the new file is being placed
   * @return this bag, with the added checksums of the new payload file
   */
  // TODO add to documentation that the pathInData cannot be a file that is already in fetch.txt
  def addPayloadFile(inputStream: InputStream)(pathInData: RelativePath): Try[Bag] = Try {
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
   * Add a payload file to the bag at the position indicated by the path relative to the `bag/data`
   * directory. If the resolved destination of this new file already exists within the bag, or if
   * the resolved destination is outside of the `bag/data` directory, this method will return a
   * `scala.util.Failure`. This method also adds the checksum of the new file to all payload manifests.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param src        the source of the new file to be added to the bag
   * @param pathInData the path relative to the `bag/data` directory where the new file is being placed
   * @return this bag, with the added checksums of the new payload file
   */
  // TODO add to documentation that the pathInData cannot be a file that is already in fetch.txt
  def addPayloadFile(src: File)(pathInData: RelativePath): Try[Bag] = Try {
    addFile(src, pathInData)(_.addPayloadFile)

    this
  }

  /**
   * Remove the payload file (relative to the `bag/data` directory) from the bag. This also removes
   * the related entries from all payload manifests. If the removal of this file causes a directory
   * to be empty, it is removed from the bag as well. However, an empty `bag/data` directory is
   * preserved.
   * If the given file does not exist or is not inside the `bag/data` directory or it is a directory,
   * a `scala.util.Failure` is returned.
   *
   * Please note that, while the file is removed from the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param pathInData the path to the file within `bag/data` that is being removed
   * @return this bag, without the payload manifest entries for the removed file
   */
  def removePayloadFile(pathInData: RelativePath): Try[Bag] = Try {
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
   * For each algorithm, list the mappings of tag file to its checksum.
   *
   * @return the mapping of tag file to its checksum, for each algorithm
   */
  def tagManifests: Map[ChecksumAlgorithm, Map[File, String]] = manifests(locBag.getTagManifests)

  /**
   * Add a tag file from an `java.io.InputStream` to the bag at the position indicated by the
   * path relative to the bag's base directory. If the resolved destination of this new file already
   * exists within the bag, or if the resolved destination is outside of the base directory, inside
   * the `bag/data` directory or if it is equal to one of the reserved files (`bagit.txt`, `bag-info.txt`,
   * `fetch.txt`, `manifest-*.txt` or `tagmanifest-*.txt`) this method will return a
   * `scala.util.Failure`. This method also adds the checksum of the new file to all tag manifests.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param inputStream the source of the new file to be added to the bag
   * @param pathInBag   the path relative to the bag's base directory where the new file is being placed
   * @return this bag, with the added checksums of the new tag file
   */
  def addTagFile(inputStream: InputStream)(pathInBag: RelativePath): Try[Bag] = Try {
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
   * Add a tag file to the bag at the position indicated by the path relative to the bag's base
   * directory. If the resolved destination of this new file already exists within the bag, or if
   * the resolved destination is outside of the base directory, inside the `bag/data` directory or
   * if it is equal to one of the reserved files (`bagit.txt`, `bag-info.txt`, `fetch.txt`,
   * `manifest-*.txt` or `tagmanifest-*.txt`) this method will return a  `scala.util.Failure`.
   * This method also adds the checksum of the new file to all tag manifests.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param src       the source of the new file to be added to the bag
   * @param pathInBag the path relative to the bag's base directory where the new file is being placed
   * @return this bag, with the added checksums of the new tag file
   */
  def addTagFile(src: File)(pathInBag: RelativePath): Try[Bag] = Try {
    addFile(src, pathInBag)(_.addTagFile)

    this
  }

  /**
   * Remove the tag file (relative to the bag's base directory) from the bag. This also removes
   * the related entries from all tag manifests. If the removal of this file causes a directory
   * to be empty, it is removed from the bag as well.
   * If the given file does not exist, or is inside the `bag/data` directory, outside/equal to the
   * base directory, or is a directory, or is one of the reserved files (`bagit.txt`, `bag-info.txt`,
   * `fetch.txt`, `manifest-*.txt` or `tagmanifest-*.txt`) a `scala.util.Failure` is returned.
   *
   * Please note that, while the file is removed from the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once `Bag.save` is called.
   *
   * @param pathInBag the path to the file within the bag's base directory that is being removed
   * @return this bag, without the tag manifest entries for the removed file
   */
  def removeTagFile(pathInBag: RelativePath): Try[Bag] = Try {
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
   * Save all changes made to this bag (using the above methods) to the file system.
   *
   * If there are no payload algorithms left (due to excessive calls to `removePayloadManifestAlgorithm`),
   * this method will fail before any writing to the file system is performed. It will also fail if
   * the bag is not writeable.
   *
   * First the `bagit.txt` file is saved, containing the bag's version and the tag file encoding.
   * Next, all payload manifests are removed and new files are created for the registered payload
   * algorithms.
   * Then, `bag-info.txt` is regenerated, including a refreshed 'Payload-Oxum', 'Bagging-Date' and
   * 'Bag-Size'.
   * Next, if there are any fetch files added to the bag, the `fetch.txt` file is created. If instead
   * the fetch files were being removed from the bag, `fetch.txt` is deleted.
   * Finally, all tag manifests are recalculated and saved in both this bag and on the file system.
   *
   * @return `scala.util.Success` if the save was performed successfully,
   *         `scala.util.Failure` otherwise
   */
  def save(): Try[Unit] = Try {
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
                     (addFileAsStream: Bag => InputStream => RelativePath => Try[Bag]): Unit = {

    def calculatePathInBagToFile(file: File)(pathInBagToFile: RelativePath): RelativePath = {
      pathInBagToFile(_) / file.name
    }

    @tailrec
    def recursion(bag: Bag, currentFile: File, pathInBagToFile: RelativePath)
                 (implicit backlog: mutable.Queue[(File, RelativePath)]): Bag = {
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

object Bag {
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
            bagInfo: Map[String, Seq[String]] = Map.empty): Try[Bag] = {
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
                     bagInfo: Map[String, Seq[String]] = Map.empty): Try[Bag] = {
    if (!payloadDir.exists)
      Failure(new NoSuchFileException(payloadDir.toString))
    else
      bagInPlace(payloadDir, algorithms, bagInfo)
  }

  private def bagInPlace(base: File,
                         algorithms: Set[ChecksumAlgorithm],
                         bagInfo: Map[String, Seq[String]]): Try[Bag] = Try {
    require(algorithms.nonEmpty,
      "At least one algorithm should be provided")

    val algos = algorithms.map(locDeconverter).asJava
    val metadata = new LocMetadata() {
      for ((key, values) <- bagInfo;
           value <- values) {
        add(key, value)
      }
    }
    new Bag(BagCreator.bagInPlace(base, algos, true, metadata))
  }

  /**
   * Reads a bag located at `baseDir` and return a `nl.knaw.dans.bag.v0.Bag` when successful.
   *
   * @param baseDir The directory containing the bag
   * @return if successful, returns a `nl.knaw.dans.bag.v0.Bag` object representing the bag located at `baseDir`
   *         else return an exception
   */
  def read(baseDir: File): Try[Bag] = Try {
    new Bag(bagReader.read(baseDir))
  }

  /**
   * Implicit conversion from a `nl.knaw.dans.bag.v0.Bag` to a `better.files.File`.
   * The `File` pointed to will be the `nl.knaw.dans.bag.v0.Bag#baseDir` of the `Bag`.
   *
   * @param bag the `Bag` object representing the bag located at its `baseDir`
   * @return the `File` pointing to the bag's `baseDir`
   */
  implicit def bagAsFile(bag: Bag): File = bag.baseDir
}
