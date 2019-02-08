package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

/**
 * Log operations are special cases of consume operations that log streamlet elements to stdout.
 * Streamlet elements will be using their toString representations and at the INFO level.
 * Log operations rely on a log sink that is provided out of the box.
 */
public class SimpleLogStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleLogStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean throttle = true;
  private static int msDelay = 500;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info("Throttle:     " + throttle);
    LOG.info("Millis Delay: " + msDelay);
    LOG.info("Nano Delay:   " + nsDelay);
    LOG.info("Msg Timeout:  " + msgTimeout);
    LOG.info("Semantics:    " + semantics);

    SimpleLogStreamlet streamletInstance = new SimpleLogStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createLogProcessingGraph(builder);

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

  private void createLogProcessingGraph(Builder builder) {

    Streamlet<Integer> intSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return StreamletUtils.generateRandomInteger(0, 100);
    });

    intSource
        .filter(i -> i % 2 == 0)
        .log();
  }
}