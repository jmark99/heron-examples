package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;

import java.util.Arrays;
import java.util.Properties;

/**
 * FlatMap Operations are like map operations but with the important difference that each element
 * of the streamlet is "flattened" into a collection type. In this example, a supplier streamlet
 * emits the same sentence over and over again; the flatMap operation transforms each sentence into
 * a Java List of individual wordsare like map operations but with the important difference that
 * each element of the streamlet is "flattened" into a collection type. In this example, a supplier
 * streamlet emits the same sentence over and over again; the flatMap operation transforms each
 * sentence into a Java List of individual words
 * <p>
 * This example is based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleFlatmapStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleFlatmapStreamlet();
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

  @Override public void createProcessingGraph(Builder builder) {
    Streamlet<String> sentenceSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "I have nothing to declare but my genius";
    });

    sentenceSource
        .setName("sentence-source")
        .flatMap((sentence) -> Arrays.asList(sentence.split("\\s+")))
        .log();
  }
}