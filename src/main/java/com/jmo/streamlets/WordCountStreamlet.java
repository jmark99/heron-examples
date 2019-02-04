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

/*
Based upon the Streamlio streamlet example found at
https://github.com/streamlio/heron-java-streamlet-api-example
 */
 public class WordCountStreamlet {

  private static final Logger LOG = Logger.getLogger(WordCountStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 100;
  private static int nsDelay = 0;

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

    WordCountStreamlet streamletInstance = new WordCountStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run WordCountStreamlet...");

    Builder builder = Builder.newBuilder();
    createWordCountProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  //
  // Topology specific setup and processing graph creation.
  //

   private static final List<String> SENTENCES_1 = Arrays.asList(
       "I have nothing to declare but my genius",
       "You can even",
       "Compassion is an action word with no boundaries",
       "To thine own self be true"
   );

   private static final List<String> SENTENCES_2 = Arrays.asList(
       "Is this the real life? Is this just fantasy?"
   );


  private void createWordCountProcessingGraph(Builder builder) {

    Streamlet<String> randomSentences1 =
        builder.newSource(() -> {
          if (addDelay) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
          return StreamletUtils.randomFromList(SENTENCES_1);
        });
    Streamlet<String> randomSentences2 =
        builder.newSource(() -> {
          if (addDelay) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
          return StreamletUtils.randomFromList(SENTENCES_2);
        });

    randomSentences1
        .union(randomSentences2)
        .map(sentence -> sentence.replace("?", ""))
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .reduceByKeyAndWindow(
            word -> word,
            word -> 1, WindowConfig.TumblingCountWindow(30),
            (x, y) -> x + y
        )
        .consume(kv -> {
          String logMessage = String.format(">>> (word: %s, count: %d",
              kv.getKey().getKey(),
              kv.getValue());
          LOG.info(logMessage);
        });
  }
 }