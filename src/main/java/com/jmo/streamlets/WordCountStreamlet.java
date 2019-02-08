package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*
Based upon the Streamlio streamlet example found at
https://github.com/streamlio/heron-java-streamlet-api-example
 */
 public class WordCountStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new WordCountStreamlet();
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

   private static final List<String> SENTENCES_1 = Arrays.asList(
       "I have nothing to declare but my genius",
       "You can even",
       "Compassion is an action word with no boundaries",
       "To thine own self be true"
   );

   private static final List<String> SENTENCES_2 = Arrays.asList(
       "Is this the real life? Is this just fantasy?"
   );


  @Override public void createProcessingGraph(Builder builder) {

    Streamlet<String> randomSentences1 =
        builder.newSource(() -> {
          if (throttle) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
          return StreamletUtils.randomFromList(SENTENCES_1);
        });

    Streamlet<String> randomSentences2 =
        builder.newSource(() -> {
          if (throttle) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
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