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

  private static String topologyName;

  /**
   * A list of company IDs to be used to generate random clicks and impressions.
   */
  private static final List<String> ADS = Arrays.asList(
      "acme",
      "blockchain-inc",
      "omnicorp"
  );

  /**
   * A list of 25 active users ("user1" through "user25").
   */
  private static final List<String> USERS = IntStream.range(1, 10)
      .mapToObj(i -> String.format("user%d", i))
      .collect(Collectors.toList());

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
      LOG.info(String.format("Instantiating impression: %s", this));
      StreamletUtils.sleep(1000);
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      StreamletUtils.sleep(250);
      return userId;
    }

    @Override
    public String toString() {
      return String.format("(adId; %s, impressionId: %s)",
          adId,
          impressionId
      );
    }
  }

  /**
   * A POJO for incoming ad clicks (generated every 50 milliseconds).
   */
  private static class AdClick implements Serializable {
    private static final long serialVersionUID = 7202766159176178988L;
    private String adId;
    private String userId;
    private String clickId;

    AdClick() {
      this.adId = StreamletUtils.randomFromList(ADS);
      this.userId = StreamletUtils.randomFromList(USERS);
      this.clickId = UUID.randomUUID().toString();
      LOG.info(String.format("Instantiating click: %s", this));
      StreamletUtils.sleep(1000);
    }

    String getAdId() {
      return adId;
    }

    String getUserId() {
      StreamletUtils.sleep(250);
      return userId;
    }

    @Override
    public String toString() {
      return String.format("(adId; %s, clickId: %s)",
          adId,
          clickId
      );
    }
  }



  public ImpressionsAndClicksStreamlet() {
    LOG.info(">>> ImpressionsAndClicksStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run ImpressionsAndClicksStreamlet...");

    Builder builder = Builder.newBuilder();

    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the impression ID and the user ID is the value.
    Streamlet<AdImpression> impressions = builder
        .newSource(AdImpression::new);

    // A KVStreamlet is produced. Each element is a KeyValue object where the key
    // is the ad ID and the user ID is the value.
    Streamlet<AdClick> clicks = builder
        .newSource(AdClick::new);

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
            WindowConfig.TumblingCountWindow(25),
            // Join type (inner join means that all elements from both streams will be included)
            JoinType.INNER,
            // For each element resulting from the join operation, a value of 1 will be provided
            // if the ad IDs match between the elements (or a value of 0 if they don't).
            (user1, user2) -> (user1.getAdId().equals(user2.getAdId())) ? 1 : 0
        )
        // The reduce function counts the number of ad clicks per user.
        .reduceByKeyAndWindow(
            // Key extractor for the reduce operation
            kv -> String.format("user-%s", kv.getKey().getKey()),
            // Value extractor for the reduce operation
            kv -> kv.getValue(),
            // Window configuration for the reduce operation
            WindowConfig.TumblingCountWindow(50),
            // A running cumulative total is calculated for each key
            (cumulative, incoming) -> cumulative + incoming
        )
        // Finally, the consumer operation provides formatted log output
        .consume(kw -> {
          LOG.info(String.format("(user: %s, clicks: %d)",
              kw.getKey().getKey(),
              kw.getValue()));
        });

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  public static void main(String[] args) throws Exception {
    ImpressionsAndClicksStreamlet streamletInstance = new ImpressionsAndClicksStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }
 }