package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

public class SimpleMapStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleMapStreamlet.class.getName());

  private static String topologyName;

  public SimpleMapStreamlet() {
    LOG.info(">>> SimpleMapStreamlet constructor");
  }

  public static void main(String[] args) throws Exception {
    SimpleMapStreamlet streamletInstance = new SimpleMapStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }

  public void runStreamlet() {
    LOG.info(">>> run SimpleMapStreamlet...");

    Builder builder = Builder.newBuilder();

    simpleMapProcessingGraph(builder);

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private void simpleMapProcessingGraph(Builder builder) {

    builder.newSource(() -> 1)
        .setName("ones")
        .map(i -> i + 12)
        .setName("add-twelve")
        .log();

    // To add a delay between the emitting of new values:
    //builder.newSource(() -> {
    //  StreamletUtils.sleep(1000);
    //  return 1;
    //})
    //  .setName("ones")
    //  .map(i -> i + 12)
    //  .setName("add-twelve")
    //  .log();
  }
}