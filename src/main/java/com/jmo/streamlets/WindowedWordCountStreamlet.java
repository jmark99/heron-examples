package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class WindowedWordCountStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new WindowedWordCountStreamlet();
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

  private static final List<String> SENTENCES = Arrays
      .asList("I have nothing to declare but my genius", "You can even",
          "Compassion is an action word with no boundaries", "To thine own self be true");

  @Override public void createProcessingGraph(Builder builder) {

    Streamlet<String> sentenceStreamlet = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return StreamletUtils.randomFromList(SENTENCES);
    });

    sentenceStreamlet
        .setName("random-sentences-source")
        // Each sentence is "flattened" into a Streamlet<String> of individual words
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .setName("flatten-into-individual-words")
        // The reduce operation performs the per-key (i.e. per-word) sum within each time window
        .reduceByKeyAndWindow(
            // The key extractor (the word is left unchanged)
            word -> word,
            // Value extractor (the value is always 1)
            word -> 1, WindowConfig.TumblingCountWindow(50), (x, y) -> x + y)
        .setName("reduce-operation")
        // The final output is logged using a user-supplied format
        .consume(kv -> {
          String logMessage = String
              .format("(word: %s, count: %d)", kv.getKey().getKey(), kv.getValue());
          LOG.info(logMessage);
        });
  }
}