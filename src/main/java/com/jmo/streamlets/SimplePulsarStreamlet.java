package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;

import java.util.Properties;

 public class SimplePulsarStreamlet extends BaseStreamlet implements IBaseStreamlet {

   public static void main(String[] args) throws Exception {
     Properties prop = new Properties();
     if (!readProperties(prop)) {
       LOG.severe("Error: Failed to read configuration properties");
       return;
     }
     IBaseStreamlet theStreamlet = new SimplePulsarStreamlet();
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
      // TODO
   }
 }