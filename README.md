# Reshape
This is the code for the implementation of Reshape on the Amber engine. Amber is the backend engine for [Texera](https://github.com/Texera/texera).

## Install packages
1. Install `Java JDK 8 or 11 (Java Development Kit)` (recommend: `adoptopenjdk`) for running the backend engine of Texera and set JAVA_HOME in your path.
2. Install `sbt` for building the project, check https://www.scala-sbt.org/1.x/docs/Setup.html. We recommend using `sdkman` to install sbt if you are using Java 8. Sbt installed using brew has problem with Java 8, as documented [here](https://stackoverflow.com/questions/61271015/sbt-fails-with-string-class-is-broken).
3. Install `Git`.
* On Windows, install the software from https://gitforwindows.org/
* On Mac and Linux, see https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
4. Install `Node.js` and `npm` (Install an LTS version instead of the latest. Currently it’s version 14.x)     
[https://nodejs.org/en/](https://nodejs.org/en/). [Use NVM to install NodeJS](https://nodesource.com/blog/installing-node-js-tutorial-using-nvm-on-mac-os-x-and-ubuntu/) because it avoids permission issues when using node.
5. Install `yarn` package manager: https://classic.yarnpkg.com/en/docs/install/

## Building the project
```console
cd core
./scripts/build.sh
```
## Running the project:
1. Open a command line and navigate to the cloned repository. If you are on Windows, you need to use [Git Bash](https://gitforwindows.org/) as a Linux bash shell in order to run shell scripts.

2. Navigate to the `core` directory
```console
cd core
```
Then build the project. 
```console
./scripts/build.sh
```
Depending on your environment, it may take a few minutes (around 2 minutes to 6 minutes).

3. Start the Texera Web server. In the `core` directory:
```console
./scripts/server.sh
```
Wait until you see the message `org.eclipse.jetty.server.Server: Started`

4. Start the Texera worker process. Open a new terminal window. In the `core` directory:
```console
./scripts/worker.sh
```
Wait until you see the message `---------Now we have 1 nodes in the cluster---------`

Note: (if `./scripts/worker.sh` gives a "permission denied error", just do `chmod 755 scripts/worker.sh` to grant an execute permission to the file).

5. Open a browser and access `http://localhost:8080`.

## To use Reshape on Amber:
1. Go to the [Constants](https://github.com/Reshape-skew-handle/reshape-on-amber/blob/main/core/amber/src/main/scala/edu/uci/ics/amber/engine/common/Constants.scala) file. It contains the configuration information.
2. Set `onlyDetectSkew = false` to enable Reshape on Amber.
3. The skew detection threshold τ can be adjused using the variable `threshold`.
