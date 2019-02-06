package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

 public class SimplePulsarStreamlet {

  private static final Logger LOG = Logger.getLogger(SimplePulsarStreamlet.class.getName());

   private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

   // Default Heron resources to be applied to the topology
   private static final double CPU = 1.5;
   private static final int GIGABYTES_OF_RAM = 8;
   private static final int NUM_CONTAINERS = 2;

   public static void main(String[] args) throws Exception {
     SimplePulsarStreamlet streamletInstance = new SimplePulsarStreamlet();
     streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
   }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimplePulsarStreamlet...");

    Builder builder = Builder.newBuilder();

    // Add processingGraph here

    Config config = Config.newBuilder()
        .setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM)
        .setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

   //
   // Topology specific setup and processing graph creation.
   //

 }