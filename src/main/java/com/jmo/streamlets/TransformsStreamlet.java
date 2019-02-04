package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class TransformsStreamlet {

  private static final Logger LOG = Logger.getLogger(TransformsStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int delay = 1;
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
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    TransformsStreamlet streamletInstance = new TransformsStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run TransformsStreamlet...");

    Builder builder = Builder.newBuilder();
    createTransformsProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 300);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  /**
   * This transformer leaves incoming values unmodified. The Consumer simply accepts incoming
   * values as-is during the transform phase.
   */
  private static class DoNothingTransformer<T> implements SerializableTransformer<T,T> {

    private static final long serialVersionUID = 3846574746643788922L;

    public void setup(Context context) {
    }

    /**
     * Here, the incoming value is accepted as-is and not changed (hence the "do nothing"
     * in the class name).
     */
    public void transform(T in, Consumer<T> consumer) {
      consumer.accept(in);
    }

    public void cleanup() {
    }
  }

  /**
   * This transformer increments incoming values by a user-supplied increment (which can also,
   * of course, be negative).
   */
  private static class IncrementTransformer implements SerializableTransformer<Integer,Integer> {
    private static final long serialVersionUID = -3198491688219997702L;
    private int increment;
    private int total;

    IncrementTransformer(int increment) {
      this.increment = increment;
    }

    public void setup(Context context) {
      context.registerMetric("InCrementMetric", 300, () -> total);
    }

    /**
     * Here, the incoming value is incremented by the value specified in the
     * transformer's constructor.
     */
    public void transform(Integer in, Consumer<Integer> consumer) {
      int incrementedValue = in + increment;
      total += increment;
      consumer.accept(incrementedValue);
    }

    public void cleanup() {
    }
  }

  private void createTransformsProcessingGraph(Builder builder) {

    /**
     * The processing graph consists of a supplier streamlet that emits
     * random integers between 1 and 100. From there, a series of transformers
     * is applied. At the end of the graph, the original value is ultimately
     * unchanged.
     */
    Streamlet<Integer> randomIntStreamlet = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current().nextInt(100);
    });

    randomIntStreamlet
        .setName("random-int-suppliers")
        .transform(new DoNothingTransformer<>())
        .transform(new IncrementTransformer(10))
        .transform(new IncrementTransformer(-7))
        .transform(new DoNothingTransformer<>())
        .transform(new IncrementTransformer(-3))
        .log();
  }
}