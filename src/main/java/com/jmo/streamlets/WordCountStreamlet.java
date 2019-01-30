package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/*
Based upon the Streamlio streamlet example found at
https://github.com/streamlio/heron-java-streamlet-api-example
 */
 public class WordCountStreamlet {

  private static final Logger LOG = Logger.getLogger(WordCountStreamlet.class.getName());

  private static String topologyName;

  public WordCountStreamlet() {
    LOG.info(">>> WordCountStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run WordCountStreamlet...");

    Builder builder = Builder.newBuilder();

    wordCountProcessingGraph(builder);

    //Config config = Config.defaultConfig();
    Config config = StreamletUtils.getAtLeastOnceConfig();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  public static void main(String[] args) throws Exception {
    WordCountStreamlet streamletInstance = new WordCountStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }

  //
  // Topology specific setup and processing graph creation.
  //

   private static final List<String> SENTENCES_1 = Arrays.asList(
       "I have nothing to declare but my genius",
       "You can even",
       "Compassion is an action word with no boundaries",
       "To thine own self be true"
   );

   private static final List<String> SENTENCES_2 = Arrays.asList(
       "Is this the real life? Is this just fantasy?"
   );


  private void wordCountProcessingGraph(Builder builder) {

    Streamlet<String> randomSentences1 =
        builder.newSource(() -> {
          StreamletUtils.sleep(10);
          return StreamletUtils.randomFromList(SENTENCES_1);
        });
    Streamlet<String> randomSentences2 =
        builder.newSource(() -> {
          StreamletUtils.sleep(10);
          return StreamletUtils.randomFromList(SENTENCES_2);
        });

    randomSentences1
        .union(randomSentences2)
        .map(sentence -> sentence.replace("?", ""))
        .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
        .reduceByKeyAndWindow(
            word -> word,
            word -> 1, WindowConfig.TumblingCountWindow(30),
            (x, y) -> x + y
        )
        .consume(kv -> {
          String logMessage = String.format(">>> (word: %s, count: %d",
              kv.getKey().getKey(),
              kv.getValue());
          LOG.info(logMessage);
        });
  }
 }