package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.logging.Logger;

/**
 * Transform a streamlet using whichever logic you'd like (useful for transformations that don't
 * neatly map onto the available operations)
 * <p>
 * In this example,  the streamlet contains a sentence that is transformed by adding a random
 * integer to the end of each. The streamlet tracks the number of transformations that take place.
 * <p>
 * This example was based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleTransformStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleTransformStreamlet.class.getName());

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
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleTransformStreamlet streamletInstance = new SimpleTransformStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    Builder builder = Builder.newBuilder();
    createTransformsProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 60);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private static class CountNumberOfItems implements SerializableTransformer<String, String> {

    private static final long serialVersionUID = 1806983116681827962L;

    private int numberOfItems;
    Map<String,Object> config;

    public void setup(Context context) {
      config = context.getConfig();
      config.put("number-of-items", 0);
    }


    public void transform(String in, Consumer<String> consumer) {
      String transformedString = in + ThreadLocalRandom.current().nextInt(1, 100);
      consumer.accept(transformedString);
      numberOfItems = (int) config.get("number-of-items");
      LOG.info("numberOfItems: " + numberOfItems);
      config.put("number-of-items", numberOfItems + 1);
    }

    public void cleanup() {
      LOG.info(
          String.format(">>> Successfully transformed: %d", numberOfItems));
    }

  }

  private void createTransformsProcessingGraph(Builder builder) {

    Streamlet<String> sentence = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "I'm thinking of the number: ";
    });

    sentence
        .transform(new CountNumberOfItems())
        .log();
  }

}