package com.jmo.streamlets;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;

public class IntegerProcessingStreamlet {

  private static final Logger LOG = Logger.getLogger(IntegerProcessingStreamlet.class.getName());

  // Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  private static String topologyName;

  public void IntegerProcessingStreamlet() {
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

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
        .build();


    new Runner().run(topologyName, config, builder);
  }



  public static void main( String[] args ) {
    IntegerProcessingStreamlet intProcessor = new IntegerProcessingStreamlet();
    if (args.length == 0) {
      System.err.print("Must provide topology name");
      System.exit(1);
    } else {
      LOG.info(">>> Using local cluster");
      topologyName = args[0];
    }
    intProcessor.runStreamlet();
  }
}


