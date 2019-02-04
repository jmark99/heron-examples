package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class WindowedWordCountStreamlet {

  private static final Logger LOG = Logger.getLogger(WindowedWordCountStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;

  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> msDelay:      " + msDelay);
    LOG.info(">>> nsDelay:      " + nsDelay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    WindowedWordCountStreamlet streamletInstance = new WindowedWordCountStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run WindowedWordCountStreamlet...");

    Builder builder = Builder.newBuilder();

    windowedWordCountProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 6*60);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private static final List<String> SENTENCES = Arrays
      .asList("I have nothing to declare but my genius", "You can even",
          "Compassion is an action word with no boundaries", "To thine own self be true");

  private void windowedWordCountProcessingGraph(Builder builder) {

    Streamlet<String> sentenceStreamlet = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return StreamletUtils.randomFromList(SENTENCES);
    });

    sentenceStreamlet
        .setName("random-sentences-source")
        // Each sentence is "flattened" into a Streamlet<String> of individual words
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .setName("flatten-into-individual-words")
        // The reduce operation performs the per-key (i.e. per-word) sum within each time window
        .reduceByKeyAndWindow(
            // The key extractor (the word is left unchanged)
            word -> word,
            // Value extractor (the value is always 1)
            word -> 1, WindowConfig.TumblingCountWindow(50), (x, y) -> x + y)
        .setName("reduce-operation")
        // The final output is logged using a user-supplied format
        .consume(kv -> {
          String logMessage = String
              .format("(word: %s, count: %d)", kv.getKey().getKey(), kv.getValue());
          LOG.info(logMessage);
        });
  }
}