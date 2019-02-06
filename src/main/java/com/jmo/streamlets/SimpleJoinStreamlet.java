package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

/**
 * Join operations unify two streamlets on a key (join operations thus require KV streamlets).
 * Each KeyValue object in a streamlet has, by definition, a key.
 */
public class SimpleJoinStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleJoinStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean throttle = true;
  private static int msDelay = 500;
  private static int nsDelay = 0;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {
    SimpleJoinStreamlet streamletInstance = new SimpleJoinStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createJoinProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, 60);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  static int cnt1 = 0;
  static int cnt2 = 1000;

  private void createJoinProcessingGraph(Builder builder) {

    Streamlet<KeyValue<String,String>> javaApi = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return new KeyValue<>("heron-api", "java-api" + cnt2++);
    });

    Streamlet<KeyValue<String,String>> streamletApi = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return new KeyValue<>("heron-api", "streamlet-api" + cnt1++);
    });

    streamletApi
        .join(javaApi,
            KeyValue::getKey,
            KeyValue::getKey,
            WindowConfig.TumblingCountWindow(5),
            JoinType.INNER,
            KeyValue::create)
        .consume(kw -> {
          System.out.println(String.format(">>> key: %s, val: %s",
              kw.getKey().getKey(), kw.getValue()));
    });

  }

}