package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

 public class SimplePulsarStreamlet {

  private static final Logger LOG = Logger.getLogger(SimplePulsarStreamlet.class.getName());

   public static void main(String[] args) throws Exception {
     SimplePulsarStreamlet streamletInstance = new SimplePulsarStreamlet();
     streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
   }

  public void runStreamlet(String topologyName) {
    LOG.info(">>> run SimplePulsarStreamlet...");

    Builder builder = Builder.newBuilder();

    // Add processingGraph here

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

   //
   // Topology specific setup and processing graph creation.
   //

 }