package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ImpressionsAndClicksStreamlet {

  private static final Logger LOG = Logger.getLogger(ImpressionsAndClicksStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int delay = 500;
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    ImpressionsAndClicksStreamlet streamletInstance = new ImpressionsAndClicksStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run ImpressionsAndClicksStreamlet...");

    Builder builder = Builder.newBuilder();
    createImpressionsAndClicksProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  /**
   * A list of company IDs to be used to generate random clicks and impressions.
   */
  private static final List<String> ADS = Arrays.asList("acme", "blockchain-inc", "omnicorp");

  /**
   * A list of 25 active users ("user1" through "user25").
   */
  private static final List<String> USERS = IntStream.range(1, 6)
      .mapToObj(i -> String.format("user%d", i)).collect(Collectors.toList());

  /**
   * A POJO for incoming ad impressions (generated every 50 milliseconds).
   */
  private static class AdImpression implements Serializable {
    private static final long serialVersionUID = 3283110635310800177L;

    private String adId;
    private String userId;
    private String impressionId;

    AdImpression() {
      this.adId = StreamletUtils.randomFromList(ADS);
      this.userId = StreamletUtils.randomFromList(USERS);
      this.impressionId = UUID.randomUUID().toString();
      if (addDelay) {
        StreamletUtils.sleep(delay);
      }
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      return userId;
    }

    @Override public String toString() {
      return String.format("(adId; %s, impressionId: %s)", adId, impressionId);
    }
  }

  /**
   * A POJO for incoming ad clicks (generated every 50 milliseconds).
   */
  private static class AdClick implements Serializable {
    private static final long serialVersionUID = 6880091079255892050L;
    private String adId;
    private String userId;
    private String clickId;

    AdClick() {
      this.adId = StreamletUtils.randomFromList(ADS);
      this.userId = StreamletUtils.randomFromList(USERS);
      this.clickId = UUID.randomUUID().toString();
      LOG.info(String.format("Instantiating click: %s", this));
      if (addDelay) {
        StreamletUtils.sleep(delay);
      }
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      return userId;
    }

    @Override public String toString() {
      return String.format("(adId; %s, clickId: %s)", adId, clickId);
    }
  }

  private void createImpressionsAndClicksProcessingGraph(Builder builder) {
    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the impression ID and the user ID is the value.
    Streamlet<AdImpression> impressions = builder.newSource(AdImpression::new);

    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the ad ID and the user ID is the value.
    Streamlet<AdClick> clicks = builder.newSource(AdClick::new);

    /**
     * Here, the impressions KVStreamlet is joined to the clicks KVStreamlet.
     */
    impressions
        // The join function here essentially provides the reduce function with a streamlet
        // of KeyValue objects where the userId matches across an impression and a click
        // (meaning that the user has clicked on the ad).
        .join(
            // The other streamlet that's being joined to
            clicks,
            // Key extractor for the impressions streamlet
            impression -> impression.getUserId(),
            // Key extractor for the clicks streamlet
            click -> click.getUserId(),
            // Window configuration for the join operation
            WindowConfig.TumblingCountWindow(5),
            // Join type (inner join means that all elements from both streams will be included)
            JoinType.INNER,
            // For each element resulting from the join operation, a value of 1 will be provided
            // if the ad IDs match between the elements (or a value of 0 if they don't).
            (user1, user2) -> (user1.getAdId().equals(user2.getAdId())) ? 1 : 0)
        // The reduce function counts the number of ad clicks per user.
        .reduceByKeyAndWindow(
            // Key extractor for the reduce operabtion
            kv -> String.format(">>> user-%s", kv.getKey().getKey()),
            // Value extractor for the reduce operation
            kv -> kv.getValue(),
            // Window configuration for the reduce operation
            WindowConfig.TumblingCountWindow(10),
            // A running cumulative total is calculated for each key
            (cumulative, incoming) -> cumulative + incoming)
        // Finally, the consumer operation provides formatted log output
        .consume(kw -> {
          LOG.info(String.format(">>> (user: %s, clicks: %d)", kw.getKey().getKey(),
              kw.getValue()));
        });
  }
}