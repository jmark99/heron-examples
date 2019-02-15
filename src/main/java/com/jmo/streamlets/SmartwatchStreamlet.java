package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.WindowConfig;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class SmartwatchStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SmartwatchStreamlet();
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

  private static final List<String> JOGGERS = Arrays.asList("bill", "ted");

  static class SmartWatchReading implements Serializable {
    private static final long serialVersionUID = -8398591517116371456L;
    private String joggerId;
    private int feetRun;

    SmartWatchReading() {
      StreamletUtils.sleep(1000);
      this.joggerId = StreamletUtils.randomFromList(JOGGERS);
      this.feetRun = ThreadLocalRandom.current().nextInt(200, 400);
    }

    String getJoggerId() {
      return joggerId;
    }

    int getFeetRun() {
      return feetRun;
    }
  }

  @Override public void createProcessingGraph(Builder builder) {

    builder.newSource(SmartWatchReading::new).setName("incoming-watch-readings")
        .reduceByKeyAndWindow(
            // Key extractor
            SmartWatchReading::getJoggerId,
            // Value extractor
            SmartWatchReading::getFeetRun,
            // The time window (1 minute of clock time)
            WindowConfig.TumblingTimeWindow(Duration.ofSeconds(10)),
            // The reduce function (produces a cumulative sum)
            (cumulative, incoming) -> cumulative + incoming)
        .setName("reduce-to-total-distance-per-jogger").map(keyWindow -> {
      // The per-key result of the previous reduce step
      long totalFeetRun = keyWindow.getValue();

      // The amount of time elapsed
      long startTime = keyWindow.getKey().getWindow().getStartTime();
      long endTime = keyWindow.getKey().getWindow().getEndTime();
      long timeLengthMillis = endTime - startTime; // Cast to float to use as denominator

      // The feet-per-minute calculation
      float feetPerMinute = totalFeetRun / (float) (timeLengthMillis / 1000);

      // Reduce to two decimal places
      String paceString = new DecimalFormat("#.##").format(feetPerMinute);

      // Return a per-jogger average pace
      return new KeyValue<>(keyWindow.getKey().getKey(), paceString);
    })
        .setName("calculate-average-speed")
        .consume(kv -> {
          String logMessage = String
          .format("(runner: %s, avgFeetPerMinute: %s)", kv.getKey(), kv.getValue());

      LOG.info(logMessage);
    });
  }
}