#!/home/mark/bin2/anaconda3/bin/python

# Quick and dirty script to run a collection of Heron Streamlets.
# Update the EXAMPLES_DIR path to match your examples location.

import begin
import subprocess
import time
from pathlib import Path
import random
import os
from pathlib import Path
import shutil

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

BASEDIR = str(Path.home())
PACKAGE = "com.jmo.streamlets"
FAT_JAR = "heron-examples-1.0-SNAPSHOT-jar-with-dependencies.jar"
EXAMPLES_DIR = BASEDIR + "/heron/heron-examples"
JAR_PATH = EXAMPLES_DIR + '/target/' + FAT_JAR
HERON = BASEDIR + "/bin/heron"
USER = os.getlogin()
TOPO_DIR = BASEDIR + "/.herondata/topologies/local/" + USER + "/"


def pause(seconds):
    print(">>> Sleep {0} seconds...".format(seconds))
    time.sleep(seconds)


def random_pause(lo, hi):
    seconds = random.randint(lo, hi)
    print(">>> Sleep {0} seconds...".format(seconds))
    time.sleep(seconds)

def clearTopologyDir():
   topo_dir = Path(TOPO_DIR)
   if topo_dir.is_dir():
       shutil.rmtree(TOPO_DIR) 

@begin.start
def run(*streamlets: "Space separated list of streamlets to execute. "
                     "If not provided use default streamlet list.",
        runtime: "number of seconds to run streamlet" = 90):

    clearTopologyDir()

    if len(streamlets) == 0:
        streamlet_list = DEFAULT_STREAMLETS
    else:
        streamlet_list = streamlets

    for streamlet in streamlet_list:
        class_name = PACKAGE + '.' + streamlet
        topology_name = streamlet

        print("=====================================================")
        print("Executing {0}...".format(streamlet))

        subprocess.run([HERON,
                        "submit",
                        "local",
                        JAR_PATH,
                        class_name,
                        topology_name,
                        "--deploy-deactivated"])
        random_pause(15, 30)

        print("Activate {0}...".format(topology_name))
        subprocess.run([HERON, "activate",  "local",  topology_name])
        pause(int(runtime))

        print("Deactivate {0}...".format(topology_name))
        subprocess.run([HERON, "deactivate",  "local",  topology_name])
        random_pause(5, 10)

        print("Kill {0}...".format(topology_name))
        subprocess.run([HERON, "kill",  "local",  topology_name])
        random_pause(5, 10)

        print("\n")
