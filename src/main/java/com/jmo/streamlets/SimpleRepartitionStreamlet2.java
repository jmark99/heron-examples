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
 */
public class SimpleRepartitionStreamlet2  extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleRepartitionStreamlet2();
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
