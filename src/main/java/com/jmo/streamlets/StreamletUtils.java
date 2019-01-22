package com.jmo.streamlets;

import org.apache.heron.streamlet.Config;

import java.util.List;
import java.util.Random;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class StreamletUtils {

  private static final Logger LOG = Logger.getLogger(StreamletUtils.class.getName());

  private static Random rand = new Random();

  private static int i = 0;

  private StreamletUtils() {
    rand = new Random(System.currentTimeMillis());
  }

  public static int getRandomInt(int upperBound) {
    return rand.nextInt(upperBound);
  }

  public static int getNextInt(int upperBound) {
    int tmp = i % upperBound;
    i++;
    return tmp;
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches the topology's name from the first command-line argument or
   * throws an exception if not present.
   */
  public static String getTopologyName(String[] args) throws Exception {
    if (args.length == 0) {
      throw new Exception("You must supply a name for the topology");
    } else {
      return args[0];
    }
  }

  /**
   * Selects a random item from a list. Used in many example source streamlets.
   */
  public static <T> T randomFromList(List<T> ls) {
    return ls.get(new Random().nextInt(ls.size()));
  }

  public static <T> T nextFromList(List<T> ls, int mod) {
    return ls.get(getNextInt(mod));
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


  // Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  static Config getAtLeastOnceConfig() {
    return Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(Config.DeliverySemantics.ATLEAST_ONCE)
        .build();
  }

}
