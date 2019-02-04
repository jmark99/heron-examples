package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class WireRequestsStreamlet {

  private static final Logger LOG = Logger.getLogger(WireRequestsStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;

  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    WireRequestsStreamlet streamletInstance = new WireRequestsStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run WireRequestsStreamlet...");

    Builder builder = Builder.newBuilder();
    createWireRequestsProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 60*6);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  /**
   * A list of current customers (some good, some bad).
   */
  static final List<String> CUSTOMERS = Arrays
      .asList("honest-tina", "honest-jeff", "scheming-dave", "scheming-linda");

  /**
   * A list of bad customers whose requests should be rejected.
   */
  private static final List<String> FRAUDULENT_CUSTOMERS = Arrays
      .asList("scheming-dave", "scheming-linda");

  /**
   * The maximum allowable amount for transfers. Requests for more than this
   * amount need to be rejected.
   */
  private static final int MAX_ALLOWABLE_AMOUNT = 500;

  /**
   * A POJO for wire requests.
   */
  private static class WireRequest implements Serializable {
    private static final long serialVersionUID = 1311441220738558016L;
    private String customerId;
    private int amount;

    WireRequest() {
      this(50);
    }

    WireRequest(long delay) {
      // The pace at which requests are generated is throttled. Different
      // throttles are applied to different bank branches.
      StreamletUtils.sleep(delay);
      this.customerId = StreamletUtils.randomFromList(CUSTOMERS);
      this.amount = ThreadLocalRandom.current().nextInt(1000);
      LOG.info(String.format("New wire request: %s", this));
    }

    String getCustomerId() {
      return customerId;
    }

    int getAmount() {
      return amount;
    }

    @Override public String toString() {
      return String.format("(customer: %s, amount: %d)", customerId, amount);
    }
  }

  /**
   * Each request is checked to make sure that requests from untrustworthy customers
   * are rejected.
   */
  private static boolean fraudDetect(WireRequest request) {
    String logMessage;

    boolean fraudulent = FRAUDULENT_CUSTOMERS.contains(request.getCustomerId());

    if (fraudulent) {
      logMessage = String.format("Rejected fraudulent customer %s", request.getCustomerId());
      LOG.warning(logMessage);
    } else {
      logMessage = String.format("Accepted request for $%d from customer %s", request.getAmount(),
          request.getCustomerId());
      LOG.info(logMessage);
    }

    return !fraudulent;
  }

  /**
   * Each request is checked to make sure that no one requests an amount over $500.
   */
  private static boolean checkRequestAmount(WireRequest request) {
    boolean sufficientBalance = request.getAmount() < MAX_ALLOWABLE_AMOUNT;

    if (!sufficientBalance) {
      LOG.warning(String.format("Rejected excessive request of $%d", request.getAmount()));
    }

    return sufficientBalance;
  }

  private void createWireRequestsProcessingGraph(Builder builder) {
    // Requests from the "quiet" bank branch (high throttling).
    Streamlet<WireRequest> quietBranch = builder
        .newSource(() -> new WireRequest(2000))
        .setNumPartitions(1)
        .setName("quiet-branch-requests")
        .filter(WireRequestsStreamlet::checkRequestAmount)
        .setName("quiet-branch-check-balance");

    // Requests from the "medium" bank branch (medium throttling).
    Streamlet<WireRequest> mediumBranch = builder
        .newSource(() -> new WireRequest(1000))
        .setNumPartitions(2)
        .setName("medium-branch-requests")
        .filter(WireRequestsStreamlet::checkRequestAmount)
        .setName("medium-branch-check-balance");

    // Requests from the "busy" bank branch (low throttling).
    Streamlet<WireRequest> busyBranch = builder
        .newSource(() -> new WireRequest(500))
        .setNumPartitions(4)
        .setName("busy-branch-requests")
        .filter(WireRequestsStreamlet::checkRequestAmount)
        .setName("busy-branch-check-balance");

    // Here, the streamlets for the three bank branches are united into one. The fraud
    // detection filter then operates on that unified streamlet.
    quietBranch
        .union(mediumBranch)
        .setNumPartitions(2)
        .setName("union-1")
        .union(busyBranch)
        .setName("union-2")
        .setNumPartitions(4)
        .filter(WireRequestsStreamlet::fraudDetect)
        .setName("all-branches-fraud-detect")
        .log();
  }

}