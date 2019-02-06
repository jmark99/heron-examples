package com.jmo.streamlets.utils;

import org.apache.heron.simulator.Simulator;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class StreamletUtils {

  public static void sleep(long millis) {
    sleep(millis, 0);
  }

  public static void sleep(long millis, int nanos) {
    try {
      Thread.sleep(millis, nanos);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches the topology's name from the first command-line argument or
   * returns null if not present.
   */
  public static String getTopologyName(String[] args) throws Exception {
    if (args.length == 0) {
      return null;
    } else {
      return args[0];
    }
  }

  /**
   * Selects a random item from a list. Used in many example source streamlets.
   */
  public static <T> T randomFromList(List<T> ls) {
    return ls.get(ThreadLocalRandom.current().nextInt(ls.size()));
  }

  /**
   * Fetches the topology's parallelism from the second-command-line
   * argument or defers to a supplied default.
   */
  public static int getParallelism(String[] args, int defaultParallelism) {
    return (args.length > 1) ? Integer.parseInt(args[1]) : defaultParallelism;
  }

  /**
   * Converts a list of integers into a comma-separated string.
   */
  public static String intListAsString(List<Integer> ls) {
    return String.join(", ", ls.stream().map(i -> i.toString()).collect(Collectors.toList()));
  }

  /**
   * Allow streamlet API topologies to run in Simulator mode.
   */
  public static void runInSimulatorMode(BuilderImpl builder, Config config,
      int timeToRunInSeconds) {
    Simulator simulator = new Simulator();
    simulator.submitTopology("test", config.getHeronConfig(), builder.build().createTopology());
    simulator.activate("test");
    StreamletUtils.sleep(timeToRunInSeconds * 1000);
    simulator.deactivate("test");
    simulator.killTopology("test");
  }

  public static void runInSimulatorMode(BuilderImpl builder, Config config) {
    runInSimulatorMode(builder, config, 300); // defaults to 5 minutes
  }

  public static int generateRandomInteger(int lo, int hi) {
    return ThreadLocalRandom.current().nextInt(lo, hi);
  }
}
