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
package nl.knaw.dans.bag

import java.io.InputStream
import java.net.{ URI, URL }
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ExecutorService

import better.files.File
import gov.loc.repository.bagit.domain.{ Version => LocVersion }
import nl.knaw.dans.bag.ChecksumAlgorithm.ChecksumAlgorithm
import nl.knaw.dans.bag.ImportOption.ImportOption
import nl.knaw.dans.bag.v0.DansV0Bag
import org.joda.time.DateTime

import scala.language.implicitConversions
import scala.util.Try

trait DansBag {

  /**
   * The base directory of the bag this object represents
   */
  val baseDir: File

  /**
   * The data directory of the bag this object represents
   */
  val data: File

  /**
   * @return the bag's `gov.loc.repository.bagit.domain.Version`, which is stored in `bagit.txt`
   */
  def bagitVersion: LocVersion

  /**
   * Change the bag's `gov.loc.repository.bagit.domain.Version` to a new `Version`.
   * Please note that this will only be synced with `bagit.txt` once [[DansBag#save]] is called.
   *
   * @param version the new `Version` of the bag
   * @return this bag, with the new `Version`
   */
  def withBagitVersion(version: LocVersion): DansBag

  /**
   * Change the bag's `gov.loc.repository.bagit.domain.Version` to a new `Version`.
   * Please note that this will only be synced with `bagit.txt` once [[DansBag#save]] is called.
   *
   * @param major major part of the version number
   * @param minor minor part of the version number
   * @return this bag, with a new `Version` comprised of the `major` and `minor` inputs
   */
  def withBagitVersion(major: Int, minor: Int): DansBag

  /**
   * @return the bag's `java.nio.charset.Charset`, which is stored in `bagit.txt`
   */
  def fileEncoding: Charset

  /**
   * Change the bag's `java.nio.charset.Charset` to a new `Charset`.
   * Please note that this will only be synced with `bagit.txt` once [[DansBag#save]] is called.
   *
   * @param charset the new `Charset` of the bag
   * @return this bag, with the new `Charset`
   */
  def withFileEncoding(charset: Charset): DansBag

  /**
   * List all entries of `bag/bag-info.txt`. Key-value pairs will be grouped on key, such that if
   * any key is listed multiple times, their values will end up together in a `Seq[String]`.
   *
   * @return a Map containing all entries of `bag/bag-info.txt`
   */
  def bagInfo: Map[String, Seq[String]]

  /**
   * Add an entry to the bagInfo. Please note that this will only be synced with `bag-info.txt`
   * once [[DansBag#save]] is called.
   *
   * @param key   the key of the new entry
   * @param value the value of the new entry
   * @return this bag, with the new entry added
   */
  def addBagInfo(key: String, value: String): DansBag

  /**
   * Remove all entries with the given key. Please note that this will only be synced with
   * `bag-info.txt` once [[DansBag#save]] is called.
   *
   * @param key the key to be removed
   * @return this bag, without the removed entries
   */
  def removeBagInfo(key: String): DansBag

  /**
   * Retrieves the value for key 'Created' from `bag-info.txt` as a parsed `org.joda.time.DateTime`
   * object. If this key has an invalid value, a `Failure` is returned instead. If the key is not
   * present, a `Success(None)` is returned instead.
   *
   * @return the `DateTime` object found in the 'Created' key in `bag-info.txt`
   */
  def created: Try[Option[DateTime]]

  /**
   * Adds the key 'Created' from `bag-info.txt`. If the key is already present, it is replaced with
   * the new value. When no argument is supplied, a default value `DateTime.now()` is used instead.
   * Please note that this will only be synced with `bag-info.txt` once [[DansBag#save]] is called.
   *
   * @param created the `DateTime` object on which the bag's content was created
   * @return this bag, with the new value for 'Created' in `bag-info.txt`
   */
  def withCreated(created: DateTime = DateTime.now()): DansBag

  /**
   * Remove the entry with key 'Created' from `bag-info.txt`.
   * Please note that this will only be synced with `bag-info.txt` once [[DansBag#save]] is called.
   *
   * @return this bag, without the removed entry
   */
  def withoutCreated(): DansBag

  /**
   * Retrieves the value for key 'Is-Version-Of' from `bag-info.txt` as a `java.net.URI` object.
   * If this key has an invalid value (if it does not match the pattern
   * `urn:uuid:[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}`), a `Failure` is returned instead.
   * If the key is not present, a `Success(None)` is returned instead.
   *
   * @return the `URI` object found in the 'Is-Version-Of' key in `bag-info.txt`
   */
  def isVersionOf: Try[Option[URI]]

  /**
   * Adds the key 'Is-Version-Of' from `bag-info.txt`. If the key is already present, it is replaced
   * with the new value. The given `java.util.UUID` is used to construct a `java.net.URI` of the
   * form `urn:uuid:[uuid]`.
   * Please note that this will only be synced with `bag-info.txt` once [[DansBag#save]] is called.
   *
   * @param uuid the `UUID` of the previous revision of this bag
   * @return this bag, with the new value for 'Is-Version-Of' in `bag-info.txt`
   */
  def withIsVersionOf(uuid: UUID): DansBag

  /**
   * Remove the entry with key 'Is-Version-Of' from `bag-info.txt`.
   * Please note that this will only be synced with `bag-info.txt` once [[DansBag#save]] is called.
   *
   * @return this bag, without the removed entry
   */
  def withoutIsVersionOf(): DansBag

  /**
   * Retrieves the value for the 'EASY-User-Account' key.
   *
   * @return the EASY user account value
   */
  def easyUserAccount: Try[Option[String]]

  /**
   * Adds `userId` under the key 'EASY-User-Account' to `bag-info.txt`. If the key is already present the
   * value is overwritten.
   *
   * Please, note that this will not be persisted to disk until [[DansBag#save]] is used.
   *
   * @return the modified DansBag
   */
  def withEasyUserAccount(userId: String): DansBag

  /**
   * Removes the entry with key 'EASY-User-Account' from `bag-info.txt`.
   *
   * Please, note that this will not be persisted to disk until [[DansBag#save]] is used.
   *
   * @return the modified DansBag
   */
  def withoutEasyUserAccount(): DansBag

  /**
   * Lists all files that are stated in `fetch.txt`.
   *
   * @return all entries listed in `fetch.txt`
   */
  def fetchFiles: Seq[FetchItem]

  /**
   * Adds the triple (url, length, path) to the `fetch.txt`.
   * If the path already exists in the fetch, or in the payload, an Exception will occur.
   * The PayloadManifests are updated with the checksum of the downloaded file. The length will be calculated as well.
   *
   * Due to calculating the checksums for all files, as well as downloading all fetch files, this
   * method may take some time to complete and return. It is therefore strongly advised to wrap
   * a call to this method in a `Promise`/`Future`, `Observable` or any other desired data structure
   * that deals with latency in a proper way.
   *
   * Please note that this will only be synced with `fetch.txt` and `manifest-<alg>.txt` once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.addFetchItem(url, Paths.get("path/to/some/file.txt"))
   * }}}
   * @param url        the url where the file is to be retrieved from
   * @param pathInData the path relative to the `bag/data` directory where the new file is virtually being placed
   * @return this bag, with the new FetchItem
   */
  def addFetchItem(url: URL, pathInData: Path): Try[DansBag]

  /**
   * Removes the fetchitem with this pathInData from the `fetch.txt` and the PayloadManifests.
   *
   * Please note that this will only be synced with `fetch.txt` and `manifest-<alg>.txt` once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.removeFetchItem(Paths.get("path/to/some/file.txt"))
   * }}}
   * @param pathInData the path relative to the `bag/data` directory of the fetchitem to be removed
   * @return this bag, without the fetchitem
   */
  def removeFetchItem(pathInData: Path): Try[DansBag]

  /**
   * Removes the fetchitem with this pathInData from the `fetch.txt` and the PayloadManifests.
   *
   * Please note that this will only be synced with `fetch.txt` and `manifest-<alg>.txt` once [[DansBag#save]] is called.
   *
   * @param url the url of the fetchitem to be removed
   * @return this bag, without the fetchitem
   */
  def removeFetchItem(url: URL): Try[DansBag]

  /**
   * Removes the fetchitem with this pathInData from the `fetch.txt` and the PayloadManifests.
   *
   * Please note that this will only be synced with `fetch.txt` and `manifest-<alg>.txt` once [[DansBag#save]] is called.
   *
   * @param item the FetchItem to be removed
   * @return this bag, without the FetchItem
   */
  def removeFetchItem(item: FetchItem): DansBag

  /**
   * Migrates a file from the payload directory to the fetch file, where it is referenced by the
   * given `URL`.
   * If the resolved path of the original file does not exist in the bag, or if the resolved path is
   * outside of the `bag/data` directory, a `Failure` is returned.
   * The `URL` is not checked for its resolvability, nor is this method responsible for uploading
   * the referenced file to the specific `URL`.
   *
   * Please note that, while the file is removed from the bag immediately, the changes to the
   * fetch file will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.replaceFileWithFetchItem(Paths.get("path/to/some/file.txt"), url)
   * }}}
   * @param pathInData the path in `bag/data` to the file that is included in the fetch file
   * @param url        the `URL` through which the file will be resolved in the future
   * @return this bag, with the added reference in the fetch file
   */
  def replaceFileWithFetchItem(pathInData: Path, url: URL): Try[DansBag]

  /**
   * Downloads a file from the list of fetch files (indicated by the relative path to `bag/data`)
   * through the corresponding `URL` and stores it at its original place. The original reference in
   * `fetch.txt` is removed.
   *
   * If the checksum(s) of the downloaded file do(es) not match the checksum(s) listed in the
   * payload manifest(s), a `Failure` will be returned. In this case, the file is not added to the
   * payload directory.
   *
   * Due to downloading the file, this method may take some time to complete and return. It is
   * therefore strongly advised to wrap a call to this method in a `Promise`/`Future`, `Observable`
   * or any other desired data structure that deals with latency in a proper way.
   *
   * Please note that this change is applied immediately and, since no changes are made to the rest
   * of the bag, there is no need to call [[DansBag#save]] for this to have full effect.
   *
   * @example
   * {{{
   *   bag.replaceFetchItemWithFile(Paths.get("path/to/some/file.txt"))
   * }}}
   * @param pathInData a relative path in `bag/data` that is listed in the fetch file and where the
   *                   file is stored on file system after being downloaded
   * @return this bag, after having downloaded the file
   */
  def replaceFetchItemWithFile(pathInData: Path): Try[DansBag]

  /**
   * Downloads a file from the list of fetch files (indicated by the `URL`) and stores it at its
   * original place, as indicated by the `fetch.txt` file. The original reference in `fetch.txt` is
   * removed.
   *
   * If the checksum(s) of the downloaded file do(es) not match the checksum(s) listed in the
   * payload manifest(s), a `Failure` will be returned.
   *
   * Due to downloading the file, this method may take some time to complete and return. It is
   * therefore strongly advised to wrap a call to this method in a `Promise`/`Future`, `Observable`
   * or any other desired data structure that deals with latency in a proper way.
   *
   * Please note that this change is applied immediately and, since no changes are made to the rest
   * of the bag, there is no need to call [[DansBag#save]] for this to have full effect.
   *
   * @param url a `URL` that is listed in the fetch file and through which the file can be downloaded
   * @return this bag, after having downloaded the file
   */
  def replaceFetchItemWithFile(url: URL): Try[DansBag]

  /**
   * Downloads a file from the list of fetch files (indicated by the [[FetchItem]]) through the
   * corresponding `URL` and stores it at its original place. The original reference in `fetch.txt`
   * is removed.
   *
   * If the checksum(s) of the downloaded file do(es) not match the checksum(s) listed in the
   * payload manifest(s), a `Failure` will be returned.
   *
   * Due to downloading the file, this method may take some time to complete and return. It is
   * therefore strongly advised to wrap a call to this method in a `Promise`/`Future`, `Observable`
   * or any other desired data structure that deals with latency in a proper way.
   *
   * Please note that this change is applied immediately and, since no changes are made to the rest
   * of the bag, there is no need to call [[DansBag#save]] for this to have full effect.
   *
   * @param item the [[FetchItem]] containing the URL and path to the file in the `bag/data` directory
   * @return this bag, after having downloaded the file
   */
  def replaceFetchItemWithFile(item: FetchItem): Try[DansBag]

  /**
   * List all algorithms that are being used in this bag to calculate the checksums of the payload files.
   *
   * @return the set of algorithms used for calculating the payload's checksums
   */
  def payloadManifestAlgorithms: Set[ChecksumAlgorithm]

  /**
   * Add an algorithm to the bag that will be used for calculating the checksums of the payload files.
   * This method can also be used to update the checksums of payload files in case some files were
   * mutated by other means than the use of this library. Supply `updateManifest = true`
   * (default false) for this behaviour.
   * In both use cases of this method, the payload manifest will be added to all tag manifests with
   * a temporary value, indicating that the real checksum for the payload manifest will only be
   * recalculated on calling [[DansBag#save]].
   * Since fetch files are also listed in all payload manifests, this method will upon encountering
   * fetch files, download them using the specified URL and calculate their checksums according to
   * the new/updated algorithm. Afterwards the downloaded files are deleted.
   *
   * Due to calculating the checksums for all files, as well as downloading all fetch files, this
   * method may take some time to complete and return. It is therefore strongly advised to wrap
   * a call to this method in a `Promise`/`Future`, `Observable` or any other desired data structure
   * that deals with latency in a proper way.
   *
   * Please note that this new/updated algorithm will only be added to the bag on the file system
   * once [[DansBag#save]] is called.
   *
   * @param checksumAlgorithm the algorithm for which the payload checksums will be added or updated
   * @param updateManifest    indicates whether it should update (`true`) or add (`false`) the algorithm
   * @return this bag, with the new algorithm added
   */
  def addPayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                                  updateManifest: Boolean = false): Try[DansBag]

  /**
   * Remove an algorithm from the bag, which was used for calculating the checksums of the payload files.
   * It also removes the payload manifest from all tagmanifests as well.
   *
   * Please note that this change will only be applied to the bag on the file system once
   * [[DansBag#save]] is called.
   *
   * @param checksumAlgorithm the algorithm to be removed from the bag with respect to the payload files
   * @return this bag, without the removed algorithm
   */
  def removePayloadManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[DansBag]

  /**
   * List all algorithms that are being used in this bag to calculate the checksums of the tag files.
   *
   * @return the set of algorithms used for calculating the tagfiles' checksums
   */
  def tagManifestAlgorithms: Set[ChecksumAlgorithm]

  /**
   * Add an algorithm to the bag that will be used for calculating the checksums of the tag files.
   * This method can also be used to update the checksums of tag files in case some files were
   * mutated by other means than the use of this library. Supply `updateManifest = true`
   * (default false) for this behaviour.
   *
   * Please note that this new/updated algorithm will only be added to the bag on the file system
   * once [[DansBag#save]] is called.
   *
   * @param checksumAlgorithm the algorithm for which the tagfiles' checksums will be added or updated
   * @param updateManifest    indicates whether it should update (`true`) or add (`false`) the algorithm
   * @return this bag, with the new algorithm added
   */
  def addTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm,
                              updateManifest: Boolean = false): Try[DansBag]

  /**
   * Remove an algorithm from the bag, which was used for calculating the checksums of the tag files.
   *
   * Please note that this change will only be applied to the bag on the file system once
   * [[DansBag#save]] is called.
   *
   * @param checksumAlgorithm the algorithm to be removed from the bag with respect to the tag files
   * @return this bag, without the removed algorithm
   */
  def removeTagManifestAlgorithm(checksumAlgorithm: ChecksumAlgorithm): Try[DansBag]

  /**
   * For each algorithm, list the mappings of payload file to its checksum.
   *
   * @return the mapping of payload file to its checksum, for each algorithm
   */
  def payloadManifests: Map[ChecksumAlgorithm, Map[File, String]]

  /**
   * Add a payload file from an `java.io.InputStream` to the bag at the position indicated by the
   * path relative to the `bag/data` directory. If the resolved destination of this new file already
   * exists within the bag, or if the resolved destination is outside of the `bag/data` directory,
   * this method will return a `scala.util.Failure`. This method also adds the checksum of the new
   * file to all payload manifests.
   *
   * Please note that fetch files are also considered part of the payload files. Therefore it is not
   * allowed to add a payload file using this method that is already declared in `fetch.txt`.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.addPayloadFile(inputStream, Paths.get("path/to/some/file.txt"))
   * }}}
   * @param inputStream the source of the new file to be added to the bag
   * @param pathInData  the path relative to the `bag/data` directory where the new file is being placed
   * @return this bag, with the added checksums of the new payload file
   */
  def addPayloadFile(inputStream: InputStream, pathInData: Path): Try[DansBag]

  /**
   * Add a payload file to the bag at the position indicated by the path relative to the `bag/data`
   * directory. If the resolved destination of this new file already exists within the bag, or if
   * the resolved destination is outside of the `bag/data` directory, this method will return a
   * `scala.util.Failure`. This method also adds the checksum of the new file to all payload manifests.
   *
   * The way of adding the payload file is determined by the implicit parameter `importOption`. By
   * default this is set to `COPY`. If set to `MOVE`, it will move the file if both source and
   * destination are on the same mount, but will copy/delete otherwise. If set to `ATOMIC_MOVE`, it
   * will move the file if both source and destination are on the same mount, but fail otherwise.
   *
   * When `src` is a directory, each file in that directory will be copied into the payload directory.
   * If `MOVE` is selected for `importOption`, importing a directory structure is not allowed.
   *
   * Please note that fetch files are also considered part of the payload files. Therefore it is not
   * allowed to add a payload file using this method that is already declared in `fetch.txt`.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   // add a single file
   *   bag.addPayloadFile(srcFile, Paths.get("path/to/some/file.txt"))
   *
   *   // add a directory of files
   *   bag.addPayloadFile(srcDir, Paths.get("path/to/some/directory"))
   * }}}
   * @param src          the source of the new file to be added to the bag
   * @param pathInData   the path relative to the `bag/data` directory where the new file is being placed
   * @param importOption the method of adding the payload file, either `COPY`, `MOVE` or `ATOMIC_MOVE`
   * @return this bag, with the added checksums of the new payload file
   */
  def addPayloadFile(src: File, pathInData: Path)
                    (implicit importOption: ImportOption = ImportOption.COPY): Try[DansBag]

  /**
   * Remove the payload file (relative to the `bag/data` directory) from the bag. This also removes
   * the related entries from all payload manifests. If the removal of this file causes a directory
   * to be empty, it is removed from the bag as well. However, an empty `bag/data` directory is
   * preserved.
   * If the given file does not exist or is not inside the `bag/data` directory or it is a directory,
   * a `scala.util.Failure` is returned.
   *
   * Please note that, while the file is removed from the bag immediately, the changes to the
   * payload manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.removePayloadFile(Paths.get("path/to/some/file.txt"))
   * }}}
   * @param pathInData the path to the file within `bag/data` that is being removed
   * @return this bag, without the payload manifest entries for the removed file
   */
  def removePayloadFile(pathInData: Path): Try[DansBag]

  /**
   * For each algorithm, list the mappings of tag file to its checksum.
   *
   * @return the mapping of tag file to its checksum, for each algorithm
   */
  def tagManifests: Map[ChecksumAlgorithm, Map[File, String]]

  /**
   * Add a tag file from an `java.io.InputStream` to the bag at the position indicated by the
   * path relative to the bag's base directory. If the resolved destination of this new file already
   * exists within the bag, or if the resolved destination is outside of the base directory, inside
   * the `bag/data` directory or if it is equal to one of the reserved files (`bagit.txt`, `bag-info.txt`,
   * `fetch.txt`, `manifest-*.txt` or `tagmanifest-*.txt`) this method will return a
   * `scala.util.Failure`. This method also adds the checksum of the new file to all tag manifests.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.addTagFile(inputStream, Paths.get("path/to/some/file.txt"))
   * }}}
   * @param inputStream the source of the new file to be added to the bag
   * @param pathInBag   the path relative to the bag's base directory where the new file is being placed
   * @return this bag, with the added checksums of the new tag file
   */
  def addTagFile(inputStream: InputStream, pathInBag: Path): Try[DansBag]

  /**
   * Add a tag file to the bag at the position indicated by the path relative to the bag's base
   * directory. If the resolved destination of this new file already exists within the bag, or if
   * the resolved destination is outside of the base directory, inside the `bag/data` directory or
   * if it is equal to one of the reserved files (`bagit.txt`, `bag-info.txt`, `fetch.txt`,
   * `manifest-*.txt` or `tagmanifest-*.txt`) this method will return a  `scala.util.Failure`.
   * This method also adds the checksum of the new file to all tag manifests.
   *
   * When `src` is a directory, each file will in that directory will be copied into the payload directory.
   *
   * Please note that, while the new file is added to the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   // add a single file
   *   bag.addTagFile(srcFile, Paths.get("path/to/some/file.txt"))
   *
   *   // add a directory of files
   *   bag.addTagFile(srcDir, Paths.get("path/to/some/directory"))
   * }}}
   * @param src       the source of the new file to be added to the bag
   * @param pathInBag the path relative to the bag's base directory where the new file is being placed
   * @return this bag, with the added checksums of the new tag file
   */
  def addTagFile(src: File, pathInBag: Path): Try[DansBag]

  /**
   * Remove the tag file (relative to the bag's base directory) from the bag. This also removes
   * the related entries from all tag manifests. If the removal of this file causes a directory
   * to be empty, it is removed from the bag as well.
   * If the given file does not exist, or is inside the `bag/data` directory, outside/equal to the
   * base directory, or is a directory, or is one of the reserved files (`bagit.txt`, `bag-info.txt`,
   * `fetch.txt`, `manifest-*.txt` or `tagmanifest-*.txt`) a `scala.util.Failure` is returned.
   *
   * Please note that, while the file is removed from the bag immediately, the changes to the
   * tag manifests will only be applied to the bag on the file system once [[DansBag#save]] is called.
   *
   * @example
   * {{{
   *   bag.removeTagFile(Paths.get("path/to/some/file.txt"))
   * }}}
   * @param pathInBag the path to the file within the bag's base directory that is being removed
   * @return this bag, without the tag manifest entries for the removed file
   */
  def removeTagFile(pathInBag: Path): Try[DansBag]

  /**
   * Save all changes made to this bag (using the above methods) to the file system.
   *
   * If there are no payload algorithms left (due to excessive calls to [[DansBag#removePayloadManifestAlgorithm]]),
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
  def save(): Try[Unit]

  /**
   * Verifies if a bag is complete.
   * According to the BagIt version 16 specs, a bag is complete when:
   *   - bagit.txt is present
   *   - data/ directory is present
   *   - at least one payload manifest exists
   *   - all fetch items are present in the bag
   *   - all files in every payload manifest must be present
   *   - all files in every tag manifest must be present
   *   - (>= V1.0 bag) all files in the payload directory are listed in all payload manifests
   *   - (< V1.0 bag) all files in the payload directory are listed in at least one payload manifest
   *
   * @return either `Unit` (if the bag is complete) or `String` (containing the error message if
   *         the bag is not complete)
   */
  def isComplete: Either[String, Unit]

  def isComplete(executor: ExecutorService): Either[String, Unit]

  /**
   * Verifies if a bag is valid.
   * According to the BagIt v16 specs, a bag is valid when:
   *   - the bag is complete
   *   - every checksum in every payload manifest has been successfully verified against the
   * contents of the corresponding file
   *   - every checksum in every tag manifest has been successfully verified against the
   * contents of the corresponding file
   *
   * Due to calculating the checksums for all files, this method may take some time to complete and
   * return. It is therefore strongly advised to wrap a call to this method in a `Promise`/`Future`,
   * `Observable` or any other desired data structure that deals with latency in a proper way.
   *
   * @return either `Unit` (if the bag is valid) or `String` (containing the error message if
   *         the bag is not valid)
   */
  def isValid: Either[String, Unit]

  def isValid(executor: ExecutorService): Either[String, Unit]
}

object DansBag {

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
   * @return if successful, returns a `nl.knaw.dans.bag.DansBag` object representing the bag located at `baseDir`
   *         else returns an exception
   */
  def empty(baseDir: File,
            algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
            bagInfo: Map[String, Seq[String]] = Map.empty): Try[DansBag] = {
    DansV0Bag.empty(baseDir, algorithms, bagInfo)
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
   * @return if successful, returns a `nl.knaw.dans.bag.DansBag` object representing the bag located at `payloadDir`
   *         else returns an exception
   */
  def createFromData(payloadDir: File,
                     algorithms: Set[ChecksumAlgorithm] = Set(ChecksumAlgorithm.SHA1),
                     bagInfo: Map[String, Seq[String]] = Map.empty): Try[DansBag] = {
    DansV0Bag.createFromData(payloadDir, algorithms, bagInfo)
  }

  /**
   * Reads a bag located at `baseDir` and return a `nl.knaw.dans.bag.DansBag` when successful.
   *
   * @param baseDir The directory containing the bag
   * @return if successful, returns a `nl.knaw.dans.bag.DansBag` object representing the bag located at `baseDir`
   *         else return an exception
   */
  def read(baseDir: File): Try[DansBag] = DansV0Bag.read(baseDir)

  /**
   * Implicit conversion from a `nl.knaw.dans.bag.DansBag` to a `better.files.File`.
   * The `File` pointed to will be the `nl.knaw.dans.bag.DansBag#baseDir` of the `DansBag`.
   *
   * @param bag the `DansBag` object representing the bag located at its `baseDir`
   * @return the `File` pointing to the bag's `baseDir`
   */
  implicit def bagAsFile(bag: DansBag): File = bag.baseDir
}
