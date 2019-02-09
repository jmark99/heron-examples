#!/bin/bash -x

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

for i in "${arr[@]}"
do
	hRunMyHeronExamples "$i" "$i"
	sleep 15
	hActivate "$i"
	sleep 90
	hDeactivate "$i"
	sleep 10
	hKill "$i"
	sleep 5
	echo "--------------------"
done
