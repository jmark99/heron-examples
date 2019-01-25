package com.jmo.streamlets;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

public class IntegerProcessingStreamlet {

  private static final Logger LOG = Logger.getLogger(IntegerProcessingStreamlet.class.getName());

  private static String topologyName;

  public IntegerProcessingStreamlet() {
    LOG.info(">>> Running IntegerProcessingStreamlet...");
  }


  public void runStreamlet() {
    LOG.info(">>> IntegerProcessingStreamlet...");

    Builder builder = Builder.newBuilder();

    Streamlet<Integer> zeroes = builder.newSource(() -> {
      StreamletUtils.sleep(1000);
      return 0;});

    builder.newSource(() -> {
      StreamletUtils.sleep(50);
      return ThreadLocalRandom.current()
          .nextInt(1, 11); })
        .setName("random-ints")
        .map(i -> i * 10)
        .setName("multi-ten")
        .union(zeroes)
        .setName("unify-streams")
        .filter(i -> i != 20)
        .setName("remove-twenties")
        .log();

    Config config = StreamletUtils.getAtLeastOnceConfig();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  public static void main( String[] args ) throws Exception {
    IntegerProcessingStreamlet intProcessor = new IntegerProcessingStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    intProcessor.runStreamlet();
  }
}


