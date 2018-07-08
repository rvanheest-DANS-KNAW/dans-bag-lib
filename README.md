dans-bag-lib
==============

Library for creating, reading and mutating DANS bags


DESCRIPTION
-----------

This library allows DANS to create, read and mutate its bags.
Every bag satisfies the [BagIt spec], as well as some additional rules created by and used in EASY.
This library allows DANS to deal with these rules in a more centralized way.

In anticipation on versioning in the '_DANS bag spec_', the current implementation is listed as '_v0_'.

[BagIt spec]: https://tools.ietf.org/html/draft-kunze-bagit-16

### Deposit
Every dataset that is stored in EASY's archive starts out as a '_deposit_': a bag and a series of
properties listed in a file called `deposit.properties`, both in the root of the deposit directory.

A deposit can be read using

```scala
import better.files.File
import nl.knaw.dans.bag.v0.Deposit

val baseDir: File = ???
val deposit = Deposit.read(baseDir)
```

In similar fashion a `Deposit` can be created as empty or from an already existing directory containing
the data files that form the bag's payload.

```scala
import better.files.File
import java.util.UUID
import nl.knaw.dans.bag.v0._
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

val baseDir: File = ???
val emptyDeposit = Deposit.empty(baseDir,
                                 Set(ChecksumAlgorithm.SHA1),
                                 Map("Created" -> Seq(DateTime.now().toString(ISODateTimeFormat.dateTime()))),
                                 State(StateLabel.DRAFT, "this deposit is in status draft"),
                                 Depositor("my-userId"),
                                 BagStore(UUID.randomUUID()))

val dataDir: File = ???
val depositFromData = Deposit.createFromData(dataDir,
                                             Set(ChecksumAlgorithm.SHA1),
                                             Map("Created" -> Seq(DateTime.now().toString(ISODateTimeFormat.dateTime()))),
                                             State(StateLabel.DRAFT, "this deposit is in status draft"),
                                             Depositor("my-userId"),
                                             BagStore(UUID.randomUUID()))
```

After being read or created, a `Deposit` object can be used to modify the `DepositProperties` and
to access the `Bag` object.

### DepositProperties
In `DepositProperties` (and `deposit.properties` on file system) the current state of the deposit
process are described. See also the [deposit properties description] for more information and a
listing of all available properties and their meaning.

[deposit properties description]: https://github.com/DANS-KNAW/easy-specs/blob/master/deposit-directory/deposit-directory.md#depositproperties

In a `Deposit` object, one can access, add and delete these properties using methods like

```scala
import scala.util.Try
import nl.knaw.dans.bag.v0._

val deposit: Deposit = ???
val doi: Option[String] = deposit.doi
val newDeposit: Deposit = deposit.withDoi("my-doi-identifier")
val newNewDeposit: Deposit = newDeposit.withoutDoi
val result: Try[Unit] = newNewDeposit.save()
```

Since the mutating methods on the deposit properties all return the new, updated `Deposit` object,
it is strongly recommended to chain consecutive mutations.
After all mutations are made to the deposit properties, the user must call `.save()` on the latest
`Deposit` object in order to write the changes to the deposit on file system.

### Bag
TODO


INSTALLATION
------------

To use this libary in a Maven-based project:

1. Include in your `pom.xml` a declaration for the DANS maven repository:

        <repositories>
            <!-- possibly other repository declartions here ... -->
            <repository>
                <id>DANS</id>
                <releases>
                    <enabled>true</enabled>
                </releases>
                <url>http://maven.dans.knaw.nl/</url>
            </repository>
        </repositories>

2. Include a dependency on this library. The version should of course be
   set to the latest version (or left out, if it is managed by an ancestor `pom.xml`).

        <dependency>
            <groupId>nl.knaw.dans.lib</groupId>
            <artifactId>dans-bag-lib_2.12</artifactId>
            <version>1.0</version>
        </dependency>
