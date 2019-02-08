package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * When you assign a number of partitions to a processing step, each step that comes after it
 * inherits that number of partitions. Thus, if you assign 5 partitions to a map operation, then any
 * mapToKV, flatMap, filter, etc. operations that come after it will also be assigned 5 partitions.
 * But you can also change the number of partitions for a processing step (as well as the number of
 * partitions for downstream operations) using repartition.
 * <p>
 * In this example, the supplier streamlet emits random integers. That operation is assigned 5
 * partitions. After the map operation, the repartition function is used to assign 2 partitions
 * to all downstream operations.
 */
public class SimpleRepartitionStreamlet  extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleRepartitionStreamlet();
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

    Streamlet<Integer> integerSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current().nextInt(0, 10);
    });

    integerSource
        .setNumPartitions(5)
        .map(i -> i + 1)
        .repartition(2)
        .filter(i -> i > 7 || i < 2)
        .consume(i -> {
          System.out.println(String.format(">>> filtered result: %d", i));
        });
  }

}
