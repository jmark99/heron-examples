package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

public class ComplexSourceStreamlet {

  private static final Logger LOG = Logger.getLogger(ComplexSourceStreamlet.class.getName());

  private static String topologyName;

  private static class IntegerSource implements Source<Integer> {

    Random rnd = new Random();
    List<Integer> intList;

    IntegerSource() {
      intList = new ArrayList<>();
    }

    /**
     * The setup functions defines the instantiation logic for the source.
     */
    public void setup(Context context) {
    }

    /**
     * The get function defines how elements for the source streamlet are
     * gotten.
     */
    public Collection<Integer> get() {
      intList.clear();
      int i = rnd.nextInt(25) + 1;
      intList.add(i + 1);
      intList.add(i + 2);
      intList.add(i + 3);
      StreamletUtils.sleep(500);
      return intList;
    }

    public void cleanup() {
    }
  }


  private static class ComplexIntegerSink<T> implements Sink<T> {
    private static final long serialVersionUID = -96514621878356324L;

    ComplexIntegerSink() {
    }

    /**
     * The setup function is called before the sink is used. Any complex
     * instantiation logic for the sink should go here.
     */
    public void setup(Context context) {
    }

    /**
     * The put function defines how each incoming streamlet element is
     * actually processed. In this case, each incoming element is converted
     * to a byte array and written to the temporary file (successful writes
     * are also logged). Any exceptions are converted to RuntimeExceptions,
     * which will effectively kill the topology.
     */
    public void put(T element) {
      LOG.info(">>>> element: " + element);
    }

    /**
     * Any cleanup logic for the sink can be applied here.
     */
    public void cleanup() {
    }
  }


  public ComplexSourceStreamlet() {
    LOG.info(">>> ComplexSourceStreamlet constructor");
  }

  public void runStreamlet() {
    LOG.info(">>> run ComplexSourceStreamlet...");

    Builder builder = Builder.newBuilder();

    Source<Integer> integerSource = new IntegerSource();

    builder.newSource(integerSource)
        .setName("integer-source")
        .map(i -> i*100)
        .toSink(new ComplexIntegerSink<>());

    Config config = StreamletUtils.getAtLeastOnceConfig();
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  public static void main(String[] args) throws Exception {
    ComplexSourceStreamlet complexTopology = new ComplexSourceStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    complexTopology.runStreamlet();
  }

}
