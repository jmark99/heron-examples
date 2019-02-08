package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;

import java.util.Properties;

/**
 * Join operations unify two streamlets on a key (join operations thus require KV streamlets).
 * Each KeyValue object in a streamlet has, by definition, a key.
 */
public class SimpleJoinStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleJoinStreamlet();
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

  private static int cnt1 = 0;
  private static int cnt2 = 1000;

  @Override public void createProcessingGraph(Builder builder) {

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