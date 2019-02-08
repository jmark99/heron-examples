package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;

import java.util.Properties;

/**
 * Consume operations are like sink operations except they don't require implementing a full sink
 * interface. Consume operations are thus suited for simple operations like formatted logging.
 * <p>
 * In this example, random integers are supplied and all even ints are formatted and logged.
 */
public class SimpleConsumeStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleConsumeStreamlet();
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

  @Override public void createProcessingGraph(Builder builder) {

    Streamlet<Integer> intSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return StreamletUtils.generateRandomInteger(0, 100);
    });

    intSource
        .filter(i -> i % 2 == 0)
        .consume(i -> {
          String message = String.format(">>> Even number found: %d", i);
          System.out.println(message);
        });
  }
}