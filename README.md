# Stand-alone Heron Streamlet Examples

This is a collection of stand-alone Heron examples that are based on samples from various sources.
These sources include:

* The Streamlet examples within the official Heron [Github repository](https://github.com/apache/incubator-heron/tree/master/examples/src/java/org/apache/heron/examples/streamlet)
* The [Streamlio wordcount example](https://github.com/streamlio/heron-java-streamlet-api-example)
* The snippits of code within the Heron online documentation:
** [The Heron Streamlet API for Java](https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/)
** [The Heron Streamlet API](https://apache.github.io/incubator-heron/docs/concepts/streamlet-api/)

These were created during my work to implement acking for Streamlet ATLEAST_ONCE topologies.

They are meant to be run against a modified version of the 0.20.x line of Heron. This modified version is the [streamlet-acks](https://github.com/jmark99/incubator-heron/tree/streamlet-acks) branch of my Heron fork and can be cloned or downloaded at this [link](https://github.com/jmark99/incubator-heron/tree/streamlet-acks).

Note that this is still a work in progress. Suggestions and/or corrections are welcome.

To run these Streamlet topologies, the following steps can be followed:

* Clone or download both repos:
** [https://github.com/jmark99/heron-examples (master branch)](https://github.com/jmark99/heron-examples))
** [https://github.com/jmark99/incubator-heron (streamlet-acks branch)](https://github.com/jmark99/incubator-heron/tree/streamlet-acks)

* cd into incubator-heron directory
* Build and install heron locally and add necessary jars to local maven repository. Note I use Ubuntu.

* The _hUpdateLocalRepo_ script in the scripts directory should perform the following steps or you can perform them manually if preferred.
** This will rebuild the heron source and install the necessary jar files to your local maven repo using the settings in the script. Update the scripts to change any necessary paths or names.

** bazel build --config=ubuntu heron/...
** bazel build --config=ubuntu scripts/packages:binpkgs
** bazel-bin/scripts/packages/heron-install.sh --user
** mvn install:install -Dfile=~/heron/incubator-heron/bazel-bin/heron/api/src/java/api-shaded.jar -DgroupId=local.heron -DartifactId=heron-api -Dversion=0.20.0 -Dpackaging=jar
** mvn install:install-file -Dfile=~/heron/incubator-heron/bazel-bin/heron/simulator/src/java/simulator-shaded.jar -DgroupId=local.heron -DartifactId=heron-api -Dversion=0.20.0 -Dpackaging=jar
** mvn install:install-file -Dfile=~/heron/incubator-heron/bazel-bin/heron/api/src/java/api-unshaded.jar -DgroupId=local.heron -DartifactId=heron-api -Dversion=0.20.0 -Dpackaging=jar

Once complete:

* cd into the directory containing the heron-examples
* mvn clean assembly:assembly (Note the POM needs to be cleaned up, but I haven't bothered to do so yet.)

Note that you can set various parameters for the examples by modifying the config.properties in the conf directory of the heron-examples project.

At this point you should be able to run the example code using the local cluster.

* heron submit local target/heron-examples-1.0-SNAPSHOT-jar-with-dependencies.jar com.jmo.streamlets.<CLASSNAME> <TOPOLOGY_NAME> --deploy-deactivated
* heron activate local <TOPOLOGY_NAME>

Allow to run as long as you like.

* heron deactivate local <TOPOLOGY_NAME>
* heron kill local <TOPOLOGY_NAME>

If you prefer python, there are a couple of python 3 programs in the _scripts_ directory to help run all of the examples and then parse and create paired output for emits/re-emits/acks.

