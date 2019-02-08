package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

import java.util.Arrays;
import java.util.Properties;

  public class SimpleReduceByKeyAndWindowStreamlet  extends BaseStreamlet implements IBaseStreamlet {

    public static void main(String[] args) throws Exception {
      Properties prop = new Properties();
      if (!readProperties(prop)) {
        LOG.severe("Error: Failed to read configuration properties");
        return;
      }
      IBaseStreamlet theStreamlet = new SimpleReduceByKeyAndWindowStreamlet();
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

      Streamlet<String> stringSource = builder.newSource(() -> {
        if (throttle) {
          StreamletUtils.sleep(msDelay, nsDelay);
        }
        return "Mary had a little lamb";
      });

      stringSource
          .flatMap(sentence -> Arrays.asList(sentence.toLowerCase().split("\\s+")))
          .reduceByKeyAndWindow(
              // Key extractor (in this case, each word acts as the key)
              word -> word,
              // Value extractor (each word appears only once, hence the value is always 1)
              word -> 1,
              // Window configuration
              WindowConfig.TumblingCountWindow(50),
              // Reduce operation (a running sum)
              (x, y) -> x + y
          )
          .consume(i -> {
            System.out.println(String.format(">>> output: %s, %s", i.getKey().getKey(),
                i.getValue()));
          });
    }
  }
