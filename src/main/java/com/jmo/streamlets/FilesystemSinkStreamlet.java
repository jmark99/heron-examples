package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class FilesystemSinkStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {

    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error reading config file");
      return;
    }
    FilesystemSinkStreamlet streamletInstance = new FilesystemSinkStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
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

  /**
   * Implements the Sink interface, which defines what happens when the toSink
   * method is invoked in a processing graph.
   */
  private static class FilesystemSink<T> implements Sink<T> {
    private static final long serialVersionUID = 3795118475903896607L;
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

  @Override public void createProcessingGraph(Builder builder) {

    // Creates a temporary file to write output into.
    File file = null;
    try {
      file = File.createTempFile("filesystem-sink-example", ".tmp");
    } catch (IOException e) {
      LOG.warning("Failed to create temporary file");
    }

    LOG.info(String.format("Ready to write to file %s", file.getAbsolutePath()));

    Streamlet<Integer> intSource = builder.newSource(() -> {
      if (throttle) {
        // This applies a "brake" that makes the processing graph write
        // to the temporary file at a reasonable, readable pace.
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current().nextInt(100);
    });

    intSource
        .setName("incoming-integers")
        // Here, the FilesystemSink implementation of the Sink
        // interface is passed to the toSink function.
        .toSink(new FilesystemSink<>(file));
  }
}