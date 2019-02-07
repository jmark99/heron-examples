package com.jmo.streamlets;

import org.apache.heron.streamlet.Builder;

public interface IBaseStreamlet {

  void runStreamlet(String topologyName);

  void createProcessingGraph(Builder builder);

}
