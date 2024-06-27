Apache Ignite 3 Pakaging
===

## Client packages

Client packages consists of the following components:

* ignite-java-client
* ignite-cpp-client (aka ignite3-client)
* ignite-dotnet-client

All of those components have packaging as:
* ZIP (task `distZip`)
* DEB (task `buildDeb`)
* RPM (task `buildRpm`)

The layout of the packages is as follows:

* ignite3-java-client:
  * DEB,RPM:
    * jars installed under the `/usr/lib/ignite3-java-client`
  * ZIP:
    * contains jars under `lib` directory
* ignite3-client:
  * DEB,RPM:
    * installs headers under `/usr/include/ignite3/*`
    * installs library under `/usr/lib/libignite3-client.so`
  * ZIP
    * contains headers under `include` direcotry
    * contains library `libignite3-client.so` under `lib`
* ignite3-dotnet-client:
  * DEB,RPM:
    * installs library, symbols and definitions under `/usr/lib/ignite3-dotnet-client`
  * ZIP:
    * contains library symbols and definitions under `lib` directory
