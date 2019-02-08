package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class RepartitionerStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new RepartitionerStreamlet();
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

  /**
   * The repartition function that determines to which partition each incoming
   * streamlet element is routed (across 8 possible partitions). Integers between 1
   * and 24 are routed to partitions 0 and 1, integers between 25 and 40 to partitions
   * 2 and 3, and so on.
   */
  private static List<Integer> repartitionStreamlet(int incomingInteger, int numPartitions) {
    List<Integer> partitions;

    if (incomingInteger >= 0 && incomingInteger < 25) {
      partitions = Arrays.asList(0, 1);
    } else if (incomingInteger > 26 && incomingInteger < 50) {
      partitions = Arrays.asList(2, 3);
    } else if (incomingInteger > 50 && incomingInteger < 75) {
      partitions = Arrays.asList(4, 5);
    } else if (incomingInteger > 76 && incomingInteger <= 100) {
      partitions = Arrays.asList(6, 7);
    } else {
      partitions = Collections.singletonList(ThreadLocalRandom.current().nextInt(0, 8));
    }

    String logMessage = String.format("Sending value %d to partitions: %s",
        incomingInteger,
        StreamletUtils.intListAsString(partitions));

    LOG.info(logMessage);

    return partitions;
  }

  @Override public void createProcessingGraph(Builder builder) {
    Streamlet<Integer> randomIntegers = builder
        .newSource(() -> {
          // Random integers are emitted every 50 milliseconds
          if (throttle) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
          return ThreadLocalRandom.current().nextInt(100);
        })
        .setNumPartitions(2)
        .setName("random-integer-source");

    randomIntegers
        // The specific repartition logic is applied here
        .repartition(8, RepartitionerStreamlet::repartitionStreamlet)
        .setName("repartition-incoming-values")
        // Here, a generic repartition logic is applied (simply
        // changing the number of partitions without specifying
        // how repartitioning will take place)
        .repartition(2)
        .setName("reduce-partitions-for-logging-operation")
        .log();
  }
 }