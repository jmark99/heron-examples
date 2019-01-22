package com.jmo.streamlets;

import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class FilesystemSinkStreamlet {

  private static final Logger LOG = Logger.getLogger(FilesystemSinkStreamlet.class.getName());

  private static String topologyName;

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

  public FilesystemSinkStreamlet() {
    LOG.info(">>> FilesystemSinkStreamlet constructor");
  }

  public void runStreamlet() throws IOException {
    LOG.info(">>> run FilesystemSinkStreamlet...");

    Builder builder = Builder.newBuilder();
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

    Config config = StreamletUtils.getAtLeastOnceConfig();
    new Runner().run(topologyName, config, builder);
  }

  public static void main(String[] args) throws Exception {
    FilesystemSinkStreamlet streamletInstance = new FilesystemSinkStreamlet();
    topologyName = StreamletUtils.getTopologyName(args);
    streamletInstance.runStreamlet();
  }
}