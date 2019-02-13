#!/home/mark/bin2/anaconda3/bin/python

# this is a quick and dirty script to parse Heron log-files looking
# for acks/re-emits/emits. It parses a list of streamlets, pulling
# out any log line containing an emit/ack/re-emit and then attempts
# to pair up the acks and re-emits with the initial emit to verify
# that messages were property sent and acknowledge. Not optimized
# at all.

import begin
import subprocess
import os
from pathlib import Path
import glob

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
USER = os.getlogin()
TOPO_DIR = BASEDIR + "/.herondata/topologies/local/" + USER + "/"


def sort_by_msg_id(input_file, output_file):
    emits = []
    acks = []
    fails = []
    with open(input_file) as infile:
        for line in infile:
            if "Emitting:" in line:
                emits.append(line.rstrip())
            elif "Acked:" in line:
                acks.append(line.rstrip())
            elif "Re-emit:" in line:
                fails.append(line.rstrip())
    pair_ids(acks, emits, fails, output_file)


def pair_ids(acks, emits, fails, output):
    emit_count = 0
    failure_count = 0
    ack_count = 0
    with open(output, 'w') as outfile:
        for line in emits:
            # output emitted line
            outfile.write("\n" + line + '\n')
            emit_count += 1
            # extract msgId from emitted line
            tokens = line.split(' ')
            msg_id = (tokens[-1]).rstrip(']')
            failure_count += parse_failures(fails, msg_id, outfile)
            ack_count += parse_acks(acks, msg_id, outfile)
    print("Number of Emitted tuples:    {0}".format(emit_count))
    print("Number of Re-emitted tuples: {0}".format(failure_count))
    print("Number of Acks:              {0}".format(ack_count))
    if emit_count != ack_count:
        print("WARNING: Emitted tuples does not equal acknowledged tuples!")


def parse_acks(acks, idval, outfile):
    ack_count = 0
    # parse ack list looking for match. If found,
    # output and remove from acks list.
    for ack in acks:
        if idval in ack:
            outfile.write(ack + "\n")
            ack_count += 1
            acks.remove(ack)
            # there should only be one ack per id, so if found, break
            break
        # if here, an ack was not found for an emit. Alert user
        #print("No ACK for {0}".format(idval))
    return ack_count


def parse_failures(fails, idval, outfile):
    failure_count = 0
    # parse all fails looking for matching ID. If
    # found, output the re-emitted line and remove from
    # fail list.
    for fail in fails:
        if idval in fail:
            outfile.write(fail + "\n")
            fails.remove(fail)
            failure_count += 1
            # can't break, as there may be multiple failures
    return failure_count


def execute(cmd):
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return result.stdout.decode('utf-8')


def delete_existing_output(output_dir, streamlet):
    parsed_ptrn = output_dir + "/" + streamlet + ".parsed*"
    paired_ptrn = output_dir + "/" + streamlet + ".paired*"
    parsed_files = glob.glob(parsed_ptrn)
    paired_files = glob.glob(paired_ptrn)
    for file in parsed_files:
        execute('rm ' + file)
    for file in paired_files:
        execute('rm ' + file)


def create_parsed_output(file, file_cnt, output_dir, streamlet):
    parsed_output = output_dir + "/" + streamlet + ".parsed." + str(file_cnt)
    file_cnt += 1
    cmd = 'cat ' + file + ' | grep "Emitting:\|Re-emit:\|Acked:" >> ' + parsed_output
    execute(cmd)
    return parsed_output, file_cnt


def assign_streamlets_to_list(streamlets):
    if len(streamlets) == 0:
        streamlet_list = DEFAULT_STREAMLETS
    else:
        streamlet_list = streamlets
    return streamlet_list


@begin.start
def run(*streamlets: "Space separated list of streamlets to check. "
                     "If not provided use default streamlet list.",
        output_dir: 'default directory containing created output data' = '/tmp'):

    streamlet_list = assign_streamlets_to_list(streamlets)

    for streamlet in streamlet_list:
        # remove any existing output from previous runs
        delete_existing_output(output_dir, streamlet)

        print("========================================================")
        print(">>> Checking counts for {0}".format(streamlet))

        # get container files in directory
        container_files = glob.glob(TOPO_DIR + streamlet + "/log-files/container*")
        file_cnt = 0
        for file in container_files:
            # skip if file does not contain emits
            matches = execute('grep -c "Emitting:" ' + file)
            if int(matches) == 0:
                continue
            print(">>> {0}".format(file.split('/')[-1]))
            parsed_output, file_cnt = create_parsed_output(file, file_cnt, output_dir, streamlet)
            # examine parsed output collecting counts and matching emits/acks
            paired_output = output_dir + "/" + streamlet + ".paired." + str(file_cnt-1)
            sort_by_msg_id(parsed_output, paired_output)
            print("\n")
        print("\n")





