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
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * Join operations unify two streamlets on a key (join operations thus require KV streamlets).
 * Each KeyValue object in a streamlet has, by definition, a key.
 */
public class SimpleJoinStreamlet2 {

  private static final Logger LOG = Logger.getLogger(SimpleJoinStreamlet2.class.getName());

  private static int msgTimeout = 45;
  private static boolean throttle = true;
  private static int msDelay = 2000;
  private static int nsDelay = 0;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {
    SimpleJoinStreamlet2 streamletInstance = new SimpleJoinStreamlet2();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 120);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private static final List<String> USERS = Arrays.asList("Joe", "Sue", "Bob");

  private static class Score implements Serializable {
    private static final long serialVersionUID = 8621493597032112829L;

    String playerUsername;
    int playerScore;

    Score() {
      this.playerUsername = StreamletUtils.randomFromList(USERS);
      this.playerScore = ThreadLocalRandom.current().nextInt(1, 25);
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
    }

    int getPlayerScore() {
      return playerScore;
    }

    String getPlayerUsername() {
      return playerUsername;
    }

    @Override public String toString() {
      return String.format("User: %s, score: %d", playerUsername, playerScore);
    }
  }

  private void createProcessingGraph(Builder builder) {

    Streamlet<Score> scores1 = builder.newSource(Score::new);
    Streamlet<Score> scores2 = builder.newSource(Score::new);

    scores1
        .join(
            scores2,
            // Key extractor for the left stream (scores1)
            Score::getPlayerUsername,
            // Key extractor for the right stream (scores2)
            Score::getPlayerUsername,
            // Window configuration
            WindowConfig.TumblingCountWindow(5),
            // Join Type
            JoinType.INNER,
            // Join function (sum scores)
            (x, y) ->
                x.getPlayerScore() + y.getPlayerScore())
        .consume(kw -> {
          LOG.info(String.format(">>> Summed Scores: (Player: %s, Score: %s)",
              kw.getKey().getKey(),
              kw.getValue()));
        });

  }

}