package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

 public class SimplePulsarStreamlet {

  private static final Logger LOG = Logger.getLogger(SimplePulsarStreamlet.class.getName());

  private static String topologyName;

  public SimplePulsarStreamlet() {
    LOG.info(">>> SimplePulsarStreamlet constructor");
  }


  public void runStreamlet() {
    LOG.info(">>> run SimplePulsarStreamlet...");

    Builder builder = Builder.newBuilder();

      //Add processingGraph here

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }



  public static void main(String[] args) throws Exception {
    SimplePulsarStreamlet streamletInstance = new SimplePulsarStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }
 }