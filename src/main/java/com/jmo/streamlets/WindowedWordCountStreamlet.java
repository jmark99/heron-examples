package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class WindowedWordCountStreamlet {

  private static final Logger LOG = Logger.getLogger(WindowedWordCountStreamlet.class.getName());

  private static String topologyName;

  private static final List<String> SENTENCES = Arrays
      .asList("I have nothing to declare but my genius", "You can even",
          "Compassion is an action word with no boundaries", "To thine own self be true");

  public WindowedWordCountStreamlet() {
    LOG.info(">>> WindowedWordCountStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run WindowedWordCountStreamlet...");

    Builder builder = Builder.newBuilder();

    builder
        // The origin of the processing graph: an indefinite series of sentences chosen
        // from the list
        .newSource(() -> StreamletUtils.randomFromList(SENTENCES))
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

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  public static void main(String[] args) throws Exception {
    WindowedWordCountStreamlet streamletInstance = new WindowedWordCountStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }
}