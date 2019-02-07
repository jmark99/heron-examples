package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FormattedOutputStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new FormattedOutputStreamlet();
    theStreamlet.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  @Override public void runStreamlet(String topologyName) {
    Builder builder = Builder.newBuilder();
    createProcessingGraph(builder);
    Config config = getConfig();
    execute(topologyName, builder, config);
  }

  void execute(String topologyName, Builder builder, Config config) {
    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config, simTimeInSecs);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

    /**
     * A list of devices emitting sensor readings ("device1" through "device100").
     */
    private static final List<String> DEVICES = IntStream.range(1, 100)
        .mapToObj(i -> String.format("device%d", i)).collect(Collectors.toList());

    /**
     * Sensor readings consist of a device ID, a temperature reading, and
     * a humidity reading. The temperature and humidity readings are
     * randomized within a range.
     */
    private static class SensorReading implements Serializable {
      private static final long serialVersionUID = 5341796532875219165L;
      private String deviceId;
      private double temperature;
      private double humidity;

      SensorReading() {
        // Readings are produced only every two seconds
        if (throttle) {
          StreamletUtils.sleep(msDelay, nsDelay);
        }
        this.deviceId = StreamletUtils.randomFromList(DEVICES);
        // Each temperature reading is a double between 70 and 100
        this.temperature = 70 + 30 * ThreadLocalRandom.current().nextDouble();
        // Each humidity reading is a percentage between 80 and 100
        this.humidity = (80 + 20 * ThreadLocalRandom.current().nextDouble()) / 100;
      }

      String getDeviceId() {
        return deviceId;
      }

      double getTemperature() {
        return temperature;
      }

      double getHumidity() {
        return humidity;
      }
    }

  @Override public void createProcessingGraph(Builder builder) {
    builder
      // The source streamlet is an indefinite series of sensor readings
      // emitted regularly
      .newSource(SensorReading::new)
      // A simple filter that excludes a percentage of the sensor readings
      .filter(reading -> reading.getHumidity() < .9 && reading.getTemperature() < 90)
      // In the consumer operation, each reading is converted to a formatted
      // string and logged
      .setName("theFilter")
      .consume(reading -> LOG.info(String
          .format("Device reading from device %s: (temp: %f, humidity: %f)",
              reading.getDeviceId(),
              reading.getTemperature(), reading.getHumidity())));
  }
}
