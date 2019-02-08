package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Streamlet;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class TransformsStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new TransformsStreamlet();
    theStreamlet.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  @Override public void runStreamlet(String topologyName) {
    Builder builder = Builder.newBuilder();
    createProcessingGraph(builder);
    Config config = getConfig();
    execute(topologyName, builder, config);
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

  @Override public void createProcessingGraph(Builder builder) {

    /**
     * The processing graph consists of a supplier streamlet that emits
     * random integers between 1 and 100. From there, a series of transformers
     * is applied. At the end of the graph, the original value is ultimately
     * unchanged.
     */
    Streamlet<Integer> randomIntStreamlet = builder.newSource(() -> {
      if (throttle) {
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