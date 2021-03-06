package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class SimpleJoinAndReduce  extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleJoinAndReduce();
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

  static final List<String> USERS = Arrays.asList("Joe", "Sue", "Bob");

  static class Score implements Serializable {
    private static final long serialVersionUID = 8621493597032112529L;

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


  @Override public void createProcessingGraph(Builder builder) {

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
            KeyValue::create)
        .reduceByKeyAndWindow(
            kv -> kv.getKey().getKey(),
            kv -> kv.getKey().getKey(),
            WindowConfig.TumblingCountWindow(10),
            (cumulative, incoming) -> cumulative + incoming)
        .consume(kw -> {
          LOG.info(String.format(">>> Summed Scores: (Player: %s, Score: %s)",
              kw.getKey().getKey(),
              kw.getValue()));
        });

  }
}
