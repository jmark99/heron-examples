package com.jmo.streamlets;

//public class SimpleSinkStreamlet {
import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.logging.Logger;

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
public class SimpleSinkStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleSinkStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean throttle = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info("Throttle:     " + throttle);
    LOG.info("Millis Delay: " + msDelay);
    LOG.info("Nano Delay:   " + nsDelay);
    LOG.info("Msg Timeout:  " + msgTimeout);
    LOG.info("Semantics:    " + semantics);

    SimpleSinkStreamlet streamletInstance = new SimpleSinkStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createProcessingGraph(builder);

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

  private void createProcessingGraph(Builder builder) {

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