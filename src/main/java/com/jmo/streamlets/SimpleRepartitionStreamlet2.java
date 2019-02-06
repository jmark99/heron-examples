package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * When you assign a number of partitions to a processing step, each step that comes after it
 * inherits that number of partitions. Thus, if you assign 5 partitions to a map operation, then any
 * mapToKV, flatMap, filter, etc. operations that come after it will also be assigned 5 partitions.
 * But you can also change the number of partitions for a processing step (as well as the number of
 * partitions for downstream operations) using repartition.
 */
public class SimpleRepartitionStreamlet2 {

  private static final Logger LOG = Logger.getLogger(SimpleRepartitionStreamlet2.class.getName());

  private static boolean throttle = true;
  private static int msDelay = 500;
  private static int nsDelay = 0;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {
    SimpleRepartitionStreamlet2 streamletInstance = new SimpleRepartitionStreamlet2();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createRepartitionProcessingGraph(builder);

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

  private void createRepartitionProcessingGraph(Builder builder) {

    Streamlet<Integer> zeroSource = builder.newSource(() -> {
      StreamletUtils.sleep(1000);
      return ThreadLocalRandom.current().nextInt(0, 10);
    });

    Streamlet<Integer> randomSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current().nextInt(1, 11);
    });

    randomSource
        .setName("random-ints")
        .setNumPartitions(3)
        .map(i -> i + 1)
        .setName("add-one")
        .repartition(3)
        .union(zeroSource)
        .setName("unify-streams")
        .repartition(2)
        .filter(i -> i != 2)
        .setName("remove-all-twos")
        .repartition(1)
        .consume(i -> {
          System.out.println(String.format("Filtered result: %d", i));
        });
  }

}
