#!/bin/bash

declare -a arr=("ComplexSourceStreamlet"
				"FormattedOutputStreamlet"
				"ImpressionsAndClicksStreamlet"
				"IntegerProcessingStreamlet"
				"RepartitionerStreamlet"
				"SimpleCloneStreamlet"
				"SimpleConsumeStreamlet"
				"SimpleFilterStreamlet"
				"SimpleFlatmapStreamlet"
				"SimpleJoinAndReduce"
				"SimpleJoinStreamlet2"
				"SimpleJoinStreamlet"
				"SimpleLogStreamlet"
				"SimpleMapStreamlet"
				"SimpleReduceByKeyAndWindowStreamlet"
				"SimpleRepartitionStreamlet2"
				"SimpleRepartitionStreamlet"
				"SimpleSinkStreamlet"
				"SimpleTransformStreamlet"
				"SimpleUnionStreamlet"
				"SmartwatchStreamlet"
				"StreamletCloneStreamlet"
				"TransformsStreamlet"
				"WindowedWordCountStreamlet"
				"WireRequestsStreamlet"
				"WordCountStreamlet"
				"FilesystemSinkStreamlet"
				)

HERONPATH="/home/mark/.herondata/topologies/local/mark"

CEMITS="/tmp/emit.cnt"
CREEMITS="/tmp/re-emit.cnt"
CACKS="/tmp/acks.cnt"

EMIT="Emit"
REEMIT="Re-emit"
ACK="Acking"

if [ -f $CEMITS ] ; then
    rm $CEMITS
fi
if [ -f $CREEMITS ] ; then
    rm $CREEMITS
fi
if [ -f $CACKS ] ; then
    rm $CACKS
fi

/bin/touch $CEMITS
/bin/touch $CREEMITS
/bin/touch $CACKS

for i in "${arr[@]}"
do
	cd "$HERONPATH/$i/log-files"
	emits=$(grep -l "$EMIT" $HERONPATH/$i/log-files/container*.log.0)
	reemits=$(grep -l "$REEMIT" $HERONPATH/$i/log-files/container*.log.0)
	acks=$(grep -l "$ACK" $HERONPATH/$i/log-files/container*.log.0)
	#$(grep "$PATTERN" $HERONPATH/$i/log-files/container*.log.0)
	echo "$emits"  >> $CEMITS
	echo "$reemits" >> $CREEMITS
	echo "$acks" >> $CACKS
 	#/bin/grep "'Re-emit' $HERONPATH/$i/log-files/container.*" >> /tmp/remitcount
done