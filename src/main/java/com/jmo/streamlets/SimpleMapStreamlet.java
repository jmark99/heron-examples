package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;

import java.util.Properties;

/**
 * Map Operations create a new streamlet by applying the supplied mapping function to each element
 * in the original streamlet.
 * <p>
 * This simple example, based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>, takes an input
 * stream of all ones and creates a new streamlet with 12 added to each value in the original
 * streamlet.
 */
public class SimpleMapStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleMapStreamlet();
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

    Streamlet<Integer> onesSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return 1;
    });

    onesSource
        .setName("ones-supplier")
        .map(i -> i + 12)
        .setName("add-twelve")
        .log();

  }
}