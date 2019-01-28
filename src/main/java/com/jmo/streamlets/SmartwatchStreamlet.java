package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class SmartwatchStreamlet {

  private static final Logger LOG = Logger.getLogger(SmartwatchStreamlet.class.getName());

  private static final List<String> JOGGERS = Arrays.asList("bill", "ted");

  public static void main(String[] args) throws Exception {
    SmartwatchStreamlet streamletInstance = new SmartwatchStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(final String topologyName) {
    LOG.info(">>> run SmartwatchStreamlet...");

    Builder builder = Builder.newBuilder();

    smartwatchProcessingGraph(builder);

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 600);
    else
      new Runner().run(topologyName, config, builder);
  }



  //
  // Topology specific setup and processing graph creation.
  //
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

  private void smartwatchProcessingGraph(Builder builder) {

    builder.newSource(SmartWatchReading::new).setName("incoming-watch-readings")
        .reduceByKeyAndWindow(
            // Key extractor
            reading -> reading.getJoggerId(),
            // Value extractor
            reading -> reading.getFeetRun(),
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
    }).setName("calculate-average-speed").consume(kv -> {
      String logMessage = String
          .format("(runner: %s, avgFeetPerMinute: %s)", kv.getKey(), kv.getValue());

      LOG.info(logMessage);
    });
  }
}