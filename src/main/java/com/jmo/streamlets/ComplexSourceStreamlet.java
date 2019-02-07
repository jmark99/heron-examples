package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class ComplexSourceStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new ComplexSourceStreamlet();
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

  private static class IntegerSource implements Source<Integer> {

    private static final long serialVersionUID = 7270252642157243930L;
    Random rnd = new Random();
    List<Integer> intList;

    IntegerSource() {
      intList = new ArrayList<>();
    }

    public void setup(Context context) {}

    /**
     * The get function defines how elements for the source streamlet are
     * gotten.
     */
    public Collection<Integer> get() {
      intList.clear();
      int i = ThreadLocalRandom.current().nextInt(25);
      intList.add(i + 1);
      intList.add(i + 2);
      intList.add(i + 3);
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return intList;
    }

    public void cleanup() {}
  }

  private static class ComplexIntegerSink<T> implements Sink<T> {

    private static final long serialVersionUID = -2565224820596857979L;

    ComplexIntegerSink() {}

    public void setup(Context context) {}

    public void put(T element) {
      LOG.info("Element: " + element);
    }

    public void cleanup() {}
  }

  @Override public void createProcessingGraph(Builder builder) {
    Source<Integer> integerSource = new IntegerSource();

    builder.newSource(integerSource)
        .setName("integer-source")
        .map(i -> i * 100)
        .setName("multiply-by-100")
        .toSink(new ComplexIntegerSink<>());
  }
}