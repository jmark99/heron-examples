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

  private static String topologyName;

  public SimpleJoinStreamlet() {
    LOG.info(">>> SimpleJoinStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run SimpleJoinStreamlet...");

    Builder builder = Builder.newBuilder();

    intBuilderProcessingGraph(builder);

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  public static void main(String[] args) throws Exception {
    SimpleJoinStreamlet streamletInstance = new SimpleJoinStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }

  //
  // Topology specific setup and processing graph creation.
  //

  private static class IntProducer implements Serializable {

    private static final long serialVersionUID = -643652260080312188L;

    private int anInt;

    IntProducer() {
      this.anInt = ThreadLocalRandom.current().nextInt(1,11);
      StreamletUtils.sleep(1000);
      LOG.info(">>> IntProducer created " + this.anInt);
    }

    int getInt() {
      return anInt;
    }

    @Override public String toString() {
      return "IntProducer{" + "anInt=" + anInt + '}';
    }
  }

  private void intBuilderProcessingGraph(Builder builder) {
    Streamlet<IntProducer> streamlet1 = builder.newSource(IntProducer::new);
    Streamlet<IntProducer> streamlet2 = builder.newSource(IntProducer::new);

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