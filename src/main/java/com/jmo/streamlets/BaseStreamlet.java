package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class  BaseStreamlet {

  static final Logger LOG = Logger.getLogger(BaseStreamlet.class.getName());

  static boolean throttle;
  static int msDelay;
  static int nsDelay;
  static int msgTimeout;
  static int simTimeInSecs;

  // Default Heron resources to be applied to the topology
  static double cpu;
  static int gigabytesOfRam;
  static int numContainers;
  static Config.DeliverySemantics semantics;

  static boolean readProperties(Properties prop) {
    try(InputStream input = new FileInputStream("conf/config.properties")) {
      prop.load(input);
      throttle = Boolean.parseBoolean(prop.getProperty("THROTTLE"));
      msDelay = Integer.parseInt(prop.getProperty("MS_DELAY"));
      nsDelay = Integer.parseInt(prop.getProperty("NS_DELAY"));
      cpu = Double.parseDouble(prop.getProperty("CPU"));
      gigabytesOfRam = Integer.parseInt(prop.getProperty("GIGABYTES_OF_RAM"));
      numContainers = Integer.parseInt(prop.getProperty("NUM_CONTAINERS"));
      semantics = Config.DeliverySemantics.valueOf(prop.getProperty("SEMANTICS"));
      msgTimeout = Integer.parseInt(prop.getProperty("MSG_TIMEOUT"));
      simTimeInSecs = Integer.parseInt(prop.getProperty("SIM_TIME_IN_SECS"));
    } catch (IOException ex) {
      return false;
    }
    return true;
  }

  Config getConfig() {
    return Config.newBuilder()
        .setNumContainers(numContainers)
        .setPerContainerRamInGigabytes(gigabytesOfRam)
        .setPerContainerCpu(cpu)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();
  }

  void execute(String topologyName, Builder builder, Config config) {
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, simTimeInSecs);
    else
      new Runner().run(topologyName, config, builder);
  }
}
