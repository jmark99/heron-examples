package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamletCloneStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new StreamletCloneStreamlet();
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
   * A list of players of the game ("player1" through "player100").
   */
  private static final List<String> PLAYERS = IntStream.range(1, 100)
      .mapToObj(i -> String.format("player%d", i))
      .collect(Collectors.toList());

  /**
   * A POJO for game scores.
   */
  private static class GameScore implements Serializable {
    private static final long serialVersionUID = 1089454399729015529L;
    private String playerId;
    private int score;

    GameScore() {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      this.playerId = StreamletUtils.randomFromList(PLAYERS);
      this.score = ThreadLocalRandom.current().nextInt(1000);
    }

    String getPlayerId() {
      return playerId;
    }

    int getScore() {
      return score;
    }
  }

  /**
   * A phony database sink. This sink doesn't actually interact with a database.
   * Instead, it logs each incoming score to stdout.
   */
  private static class DatabaseSink implements Sink<GameScore> {
    private static final long serialVersionUID = 5544736723673011054L;

    private void saveToDatabase(GameScore score) {
      LOG.info(">>>> saved to database: " + score.score);
    }

    public void setup(Context context) {
    }

    public void put(GameScore score) {
      String logMessage = String.format(">>>> Saving a score of %d for player %s to the database",
          score.getScore(),
          score.getPlayerId());
      LOG.info(logMessage);
      saveToDatabase(score);
    }

    public void cleanup() {
    }
  }

  /**
   * A logging sink that simply prints a formatted log message for each incoming score.
   */
  private static class FormattedLogSink implements Sink<GameScore> {

    private static final long serialVersionUID = -11392565006864298L;

    public void setup(Context context) {
    }

    public void put(GameScore score) {
      String logMessage = String.format(">>>> The current score for player %s is %d",
          score.getPlayerId(),
          score.getScore());
      LOG.info(logMessage);
    }

    public void cleanup() {
    }
  }

  @Override public void createProcessingGraph(Builder builder) {
    /**
     * A supplier streamlet of random GameScore objects is cloned into two
     * separate streamlets.
     */
    List<Streamlet<GameScore>> splitGameScoreStreamlet = builder
        .newSource(GameScore::new)
        .clone(2);

    /**
     * Elements in the first cloned streamlet go to the database sink.
     */
    splitGameScoreStreamlet.get(0)
        .toSink(new DatabaseSink());

    /**
     * Elements in the second cloned streamlet go to the logging sink.
     */
    splitGameScoreStreamlet.get(1)
        .toSink(new FormattedLogSink());
  }
}
