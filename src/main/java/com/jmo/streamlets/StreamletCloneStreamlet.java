package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StreamletCloneStreamlet {

  private static final Logger LOG = Logger.getLogger(StreamletCloneStreamlet.class.getName());

  private static String topologyName;


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
      StreamletUtils.sleep(1000);
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
    private static final long serialVersionUID = 1251089445039059977L;
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


  public StreamletCloneStreamlet() {
    LOG.info(">>> StreamletCloneStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run StreamletCloneStreamlet...");

    Builder builder = Builder.newBuilder();

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


    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  public static void main(String[] args) throws Exception {
    StreamletCloneStreamlet streamletInstance = new StreamletCloneStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }
 }
