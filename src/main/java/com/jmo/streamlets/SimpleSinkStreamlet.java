package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;

import java.util.Properties;

/**
 * In processing graphs like the ones you build using the Heron Streamlet API, sinks are essentially
 * the terminal points in your graph, where your processing logic comes to an end. A processing
 * graph can end with writing to a database, publishing to a topic in a pub-sub messaging system,
 * and so on. With the Streamlet API, you can implement your own custom sinks.
 * <p>
 * In this example, the sink fetches the name of the enclosing streamlet from the context passed in
 * the setup method. The put method specifies how the sink handles each element that is received
 * (in this case, a formatted message is logged to stdout). The cleanup method enables you to
 * specify what happens after the element has been processed by the sink.
 */
public class SimpleSinkStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleSinkStreamlet();
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

  private static class FormattedLogSink<T> implements Sink<T> {
    private static final long serialVersionUID = 5420852416868715913L;
    private String streamletName;

    public void setup(Context context) {
      streamletName = context.getStreamName();
    }

    public void put(T element) {
      String message = String.format("Streamlet %s has produced an element with a value of: '%s'",
          streamletName,
          element.toString());
      System.out.println(message);
    }

    public void cleanup() {}
  }

  @Override public void createProcessingGraph(Builder builder) {

    final Streamlet<String> stringSource = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return "Here is a string to be passed to the sink ";
    });
    stringSource
        .toSink(new FormattedLogSink<>());
  }
}