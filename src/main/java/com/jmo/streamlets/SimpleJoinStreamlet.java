package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class SimpleJoinStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleJoinStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static int delay = 500;
  private static boolean addDelay = true;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> delay:        " + delay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleJoinStreamlet streamletInstance = new SimpleJoinStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimpleJoinStreamlet...");

    Builder builder = Builder.newBuilder();
    createIntegerProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .setUserConfig("topology.droptuples.upon.backpressure", false)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  //
  // Topology specific setup and processing graph creation.
  //

  private static class IntProducer implements Serializable {

    private static final long serialVersionUID = -643652260080312188L;

    private int anInt;

    IntProducer() {
      this.anInt = ThreadLocalRandom.current().nextInt(1,11);
      if (addDelay) {
        StreamletUtils.sleep(delay);
      }
      LOG.info(">>> IntProducer created " + this.anInt);
    }

    IntProducer(int lo, int hi) {
      this.anInt = ThreadLocalRandom.current().nextInt(lo,hi);
      if (addDelay) {
        StreamletUtils.sleep(delay);
      }
      LOG.info(">>> IntProducer created " + this.anInt);
    }

    int getInt() {
      return anInt;
    }

    @Override public String toString() {
      return "IntProducer{" + "anInt=" + anInt + '}';
    }
  }

  private void createIntegerProcessingGraph(Builder builder) {
    Streamlet<IntProducer> streamlet1
        = builder.newSource(() -> new IntProducer(1, 11));
    Streamlet<IntProducer> streamlet2
        = builder.newSource(() -> new IntProducer(101, 111));

    streamlet1.join(
        streamlet2,
        stream1 -> stream1.getInt(),
        stream2 -> stream2.getInt(),
        WindowConfig.TumblingCountWindow(10),
        JoinType.INNER,
        (num1, num2) -> (num1.getInt() == num2.getInt()) ? num1.getInt() + num2.getInt() : 0)
        .reduceByKeyAndWindow(
            kv -> String.format(">>> user-%s", kv.getKey().getKey()),
            kv -> kv.getValue(),
            WindowConfig.TumblingCountWindow(20),
            (cumulative, incoming) -> cumulative + incoming)
        .consume(kw -> {
          LOG.info(String.format(">>> (user: %s, ints: %d)", kw.getKey().getKey(), kw.getValue()));
        });
  }

}