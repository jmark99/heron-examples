package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class FilesystemSinkStreamlet {

  private static final Logger LOG = Logger.getLogger(FilesystemSinkStreamlet.class.getName());

  private static boolean throttle;
  private static int msDelay;
  private static int nsDelay;
  private static int msgTimeout;

  // Default Heron resources to be applied to the topology
  private static double cpu;
  private static int gigabytesOfRam;
  private static int numContainers;
  private static Config.DeliverySemantics semantics;

  public static void main(String[] args) throws Exception {

    Properties prop = new Properties();
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
    } catch (IOException ex) {
      LOG.severe("Error reading config file");
      return;
    }

    FilesystemSinkStreamlet streamletInstance = new FilesystemSinkStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) throws IOException {
    LOG.info(">>> run FilesystemSinkStreamlet...");

    Builder builder = Builder.newBuilder();
    createFilesystemSinkProcessingGraph(builder);

    Config config = Config.newBuilder()
        .setNumContainers(numContainers)
        .setPerContainerRamInGigabytes(gigabytesOfRam)
        .setPerContainerCpu(cpu)
        .setDeliverySemantics(semantics)
        .setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

  /**
   * Implements the Sink interface, which defines what happens when the toSink
   * method is invoked in a processing graph.
   */
  private static class FilesystemSink<T> implements Sink<T> {
    private static final long serialVersionUID = -96514621878356224L;
    private Path tempFilePath;
    private File tempFile;

    FilesystemSink(File f) {
      LOG.info(">>>> Using FilesystemSink(" + f.getAbsolutePath() + ")");
      this.tempFile = f;
    }

    /**
     * The setup function is called before the sink is used. Any complex
     * instantiation logic for the sink should go here.
     */
    public void setup(Context context) {
      this.tempFilePath = Paths.get(tempFile.toURI());
    }

    /**
     * The put function defines how each incoming streamlet element is
     * actually processed. In this case, each incoming element is converted
     * to a byte array and written to the temporary file (successful writes
     * are also logged). Any exceptions are converted to RuntimeExceptions,
     * which will effectively kill the topology.
     */
    public void put(T element) {
      byte[] bytes = String.format("%s\n", element.toString()).getBytes();

      try {
        Files.write(tempFilePath, bytes, StandardOpenOption.APPEND);
        LOG.info(
            String.format(">>>> Wrote %s to %s", new String(bytes), tempFilePath.toAbsolutePath()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Any cleanup logic for the sink can be applied here.
     */
    public void cleanup() {
    }
  }

  private void createFilesystemSinkProcessingGraph(Builder builder) throws IOException {
    // Creates a temporary file to write output into.
    File file = File.createTempFile("filesystem-sink-example", ".tmp");

    LOG.info(String.format("Ready to write to file %s", file.getAbsolutePath()));

    builder.newSource(() -> {
      // This applies a "brake" that makes the processing graph write
      // to the temporary file at a reasonable, readable pace.
      StreamletUtils.sleep(500);
      return ThreadLocalRandom.current().nextInt(100);
    }).setName("incoming-integers")
        // Here, the FilesystemSink implementation of the Sink
        // interface is passed to the toSink function.
        .toSink(new FilesystemSink<>(file));
  }
}