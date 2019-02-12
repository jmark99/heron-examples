#!/home/mark/bin2/anaconda3/bin/python

import begin
import subprocess
import time
import os
from pathlib import Path

DEFAULT_STREAMLETS = ["ComplexSourceStreamlet",
                      "FormattedOutputStreamlet",
                      "ImpressionsAndClicksStreamlet",
                      "IntegerProcessingStreamlet",
                      "RepartitionerStreamlet",
                      "SimpleCloneStreamlet",
                      "SimpleConsumeStreamlet",
                      "SimpleFilterStreamlet",
                      "SimpleFlatmapStreamlet",
                      "SimpleJoinAndReduce",
                      "SimpleJoinStreamlet2",
                      "SimpleJoinStreamlet",
                      "SimpleLogStreamlet",
                      "SimpleMapStreamlet",
                      "SimpleReduceByKeyAndWindowStreamlet",
                      "SimpleRepartitionStreamlet2",
                      "SimpleRepartitionStreamlet",
                      "SimpleSinkStreamlet",
                      "SimpleTransformStreamlet",
                      "SimpleUnionStreamlet",
                      "SmartwatchStreamlet",
                      "StreamletCloneStreamlet",
                      "TransformsStreamlet",
                      "WindowedWordCountStreamlet",
                      "WireRequestsStreamlet",
                      "WordCountStreamlet",
                      "FilesystemSinkStreamlet"
                      ]

HOME_DIR = str(Path.home())
USER = os.getlogin()
BASEDIR = HOME_DIR + "/"

RUN_SCRIPT = BASEDIR + "bin/scripts/hRunMyHeronExamples"
TOPO_DIR = BASEDIR + ".herondata/topologies/local/" + USER + "/"
HERON = BASEDIR + "bin/heron"


def pause(seconds):
    print(">>> Sleep {0} seconds...".format(seconds))
    time.sleep(seconds)


@begin.start
def run(*streamlets: "Space separated list of streamlets to execute. "
                     "If not provided use default streamlet list.",
        runtime: "number of seconds to run streamlet" = 90):

    if len(streamlets) == 0:
        streamlet_list = DEFAULT_STREAMLETS
    else:
        streamlet_list = streamlets

    for streamlet in streamlet_list:
        class_name = "com.jmo.streamlets." + streamlet
        topology_name = streamlet

        print("=====================================================")
        print("Executing {0}...".format(streamlet))

        subprocess.run([HERON,
                        "submit",
                        "local",
                        BASEDIR + "heron/heron-examples/target/heron-examples-1.0-SNAPSHOT-jar-with-dependencies.jar",
                        class_name,
                        topology_name,
                        "--deploy-deactivated"])
        pause(15)

        print("Activate {0}...".format(topology_name))
        subprocess.run([HERON, "activate",  "local",  topology_name])
        pause(int(runtime))

        print("Deactivate {0}...".format(topology_name))
        subprocess.run([HERON, "deactivate",  "local",  topology_name])
        pause(5)

        print("Kill {0}...".format(topology_name))
        subprocess.run([HERON, "kill",  "local",  topology_name])
        pause(5)

        print("\n\n")
