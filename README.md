dans-bag-lib
==============

Library for reading and writing DANS bags


DESCRIPTION
-----------

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
