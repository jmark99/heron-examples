package com.jmo.streamlets;

//public class SimpleReduceByKeyAndWindowStreamlet {
import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.Arrays;
import java.util.logging.Logger;

  public class SimpleReduceByKeyAndWindowStreamlet {

    private static final Logger LOG = Logger.getLogger(SimpleReduceByKeyAndWindowStreamlet.class.getName());

    private static int msgTimeout = 30;
    private static boolean throttle = true;
    private static int msDelay = 500;
    private static int nsDelay = 1;
    private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

    // Default Heron resources to be applied to the topology
    private static final double CPU = 1.5;
    private static final int GIGABYTES_OF_RAM = 8;
    private static final int NUM_CONTAINERS = 2;

    public static void main(String[] args) throws Exception {

      LOG.info("Throttle:     " + throttle);
      LOG.info("Msg Timeout:  " + msgTimeout);
      LOG.info("Semantics:    " + semantics);

      SimpleReduceByKeyAndWindowStreamlet streamletInstance = new SimpleReduceByKeyAndWindowStreamlet();
      streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
    }

    public void runStreamlet(String topologyName) {

      Builder builder = Builder.newBuilder();
      createProcessingGraph(builder);

      Config config = Config.newBuilder()
          .setNumContainers(NUM_CONTAINERS)
          .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
          .setPerContainerCpu(CPU)
          .setDeliverySemantics(semantics)
          .setUserConfig("topology.message.timeout.secs", msgTimeout)
          .setUserConfig("topology.droptuples.upon.backpressure", false)
          .build();

      if (topologyName == null)
        StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 60);
      else
        new Runner().run(topologyName, config, builder);
    }

    //
    // Topology specific setup and processing graph creation.
    //

    private void createProcessingGraph(Builder builder) {

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
              WindowConfig.TumblingCountWindow(13),
              // Reduce operation (a running sum)
              (x, y) -> x + y
          )
          .consume(i -> {
            System.out.println(String.format(">>> output: %s, %s", i.getKey().getKey(),
                i.getValue()));
          });
    }
  }
