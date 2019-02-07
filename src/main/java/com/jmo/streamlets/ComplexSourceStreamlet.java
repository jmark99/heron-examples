package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class ComplexSourceStreamlet {

  private static final Logger LOG = Logger.getLogger(ComplexSourceStreamlet.class.getName());

  private static boolean throttle;
  private static int msDelay;
  private static int nsDelay;

  // Default Heron resources to be applied to the topology
  private static double cpu;
  private static int gigabytesOfRam;
  private static int numContainers;
  private static Config.DeliverySemantics semantics;

  public ComplexSourceStreamlet() {}

  public static void main(String[] args) throws Exception {

    Properties prop = new Properties();
    try(InputStream input = new FileInputStream("conf/config.properties")) {
      prop.load(input);
      throttle = Boolean.parseBoolean(prop.getProperty("THROTTLE"));
      msDelay = Integer.parseInt(prop.getProperty("MS_DELAY"));
      nsDelay = Integer.parseInt(prop.getProperty("NS_DELAY"g));
      cpu = Double.parseDouble(prop.getProperty("CPU"));
      gigabytesOfRam = Integer.parseInt(prop.getProperty("GIGABYTES_OF_RAM"));
      numContainers = Integer.parseInt(prop.getProperty("NUM_CONTAINERS"));
      semantics = Config.DeliverySemantics.valueOf(prop.getProperty("SEMANTICS"));
    } catch (IOException ex) {
      LOG.severe("Error reading config file");
      return;
    }

    ComplexSourceStreamlet complexTopology = new ComplexSourceStreamlet();
    complexTopology.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) {

    Builder builder = Builder.newBuilder();
    createProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(numContainers)
        .setPerContainerRamInGigabytes(gigabytesOfRam)
        .setPerContainerCpu(cpu)
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

  private void createProcessingGraph(Builder builder) {

    Source<Integer> integerSource = new IntegerSource();

    builder.newSource(integerSource)
        .setName("integer-source")
        .map(i -> i * 100)
        .setName("multiply-by-100")
        .toSink(new ComplexIntegerSink<>());
  }
}
