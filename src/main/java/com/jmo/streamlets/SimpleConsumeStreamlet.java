package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

/**
 * Consume operations are like sink operations except they don't require implementing a full sink
 * interface. Consume operations are thus suited for simple operations like formatted logging.
 * <p>
 * In this example, random integers are supplied and all even ints are formatted and logged.
 */
public class SimpleConsumeStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleConsumeStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean throttle = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info("throttle:     " + throttle);
    LOG.info("msDelay:      " + msDelay);
    LOG.info("nsDelay:      " + nsDelay);
    LOG.info("msgTimeout:   " + msgTimeout);
    LOG.info("semantics:    " + semantics);

    SimpleConsumeStreamlet streamletInstance = new SimpleConsumeStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createConsumeProcessingGraph(builder);

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

  private void createConsumeProcessingGraph(Builder builder) {

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