package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Streamlet;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

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
public class SimpleTransformStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleTransformStreamlet();
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

  @Override public void createProcessingGraph(Builder builder) {

    Streamlet<String> sentence = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "I'm thinking of the number: ";
    });

    sentence
        .transform(new CountNumberOfItems())
        .log();
  }

}