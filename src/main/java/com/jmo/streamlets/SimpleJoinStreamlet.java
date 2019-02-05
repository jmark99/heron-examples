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
  private static boolean addDelay = true;
  private static int msDelay = 500;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleJoinStreamlet streamletInstance = new SimpleJoinStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimpleJoinStreamlet...");

    Builder builder = Builder.newBuilder();
    createJoinrocessingGraph(builder);

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

  private void createJoinrocessingGraph(Builder builder) {

    //KeyValue<String,String> topologyKeyValue = new KeyValue<>("heron-api", "topology-api");

    Streamlet<KeyValue<String,String>> javaApi
        = builder.newSource(() -> {
          if (addDelay) {
            StreamletUtils.sleep(msDelay, nsDelay);
          }
          return new KeyValue<>("heron-api", "java-api" + cnt2++);
    });

    Streamlet<KeyValue<String,String>> streamletApi
        = builder.newSource(() -> {
      if (addDelay) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return new KeyValue<>("heron-api", "streamlet-api" + cnt1++);
    });

    streamletApi
        .join(
            javaApi,
            (sapi -> sapi.getKey()),
            (japi -> japi.getKey()),
            WindowConfig.TumblingCountWindow(5),
            JoinType.INNER, KeyValue::create)
        .consume(kw -> {
          LOG.info(String.format(">>> key: %s, val: %s",
              kw.getKey().getKey(),
              kw.getValue()));
        });

  }

}