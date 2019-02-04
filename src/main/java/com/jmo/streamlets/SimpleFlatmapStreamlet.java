package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * FlatMap Operations are like map operations but with the important difference that each element
 * of the streamlet is "flattened" into a collection type. In this example, a supplier streamlet
 * emits the same sentence over and over again; the flatMap operation transforms each sentence into
 * a Java List of individual wordsare like map operations but with the important difference that
 * each element of the streamlet is "flattened" into a collection type. In this example, a supplier
 * streamlet emits the same sentence over and over again; the flatMap operation transforms each
 * sentence into a Java List of individual words
 * <p>
 * This example is based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleFlatmapStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleFlatmapStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int delay = 1;
  private static int msDelay = 100;
  private static int nsDelay = 0;
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleFlatmapStreamlet streamletInstance = new SimpleFlatmapStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimpleFlatmapStreamlet...");

    Builder builder = Builder.newBuilder();
    createFlatmapProcessingGraph(builder);

    Config config = Config.newBuilder().setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM).setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics).setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private void createFlatmapProcessingGraph(Builder builder) {
    Streamlet<String> sentenceSource = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "I have nothing to declare but my genius";
    });

    sentenceSource
        .setName("sentence-source")
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
        .log();
  }
}