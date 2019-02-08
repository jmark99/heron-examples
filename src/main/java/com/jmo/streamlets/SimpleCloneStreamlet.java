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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Clone operations enable you to create any number of "copies" of a streamlet. Each of the "copy"
 * streamlets contains all the elements of the original and can be manipulated just like the
 * original streamlet.
 * <p>
 * In this example, a streamlet of random integers between 1 and 100 is split into 5 identical
 * streamlets.
 * <p>
 * This example is based upon the snippet at
 * <a https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/>
 * https://apache.github.io/incubator-heron/docs/developers/java/streamlet-api/</a>,
 */
public class SimpleCloneStreamlet extends BaseStreamlet implements IBaseStreamlet {

  public static void main(String[] args) throws Exception {
    Properties prop = new Properties();
    if (!readProperties(prop)) {
      LOG.severe("Error: Failed to read configuration properties");
      return;
    }
    IBaseStreamlet theStreamlet = new SimpleCloneStreamlet();
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

  private static class FilesystemSink<T> implements Sink<T> {
    private static final long serialVersionUID = -96514621878356224L;
    private Path tempFilePath;
    private File tempFile;

    FilesystemSink(File f) {
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

    File file0 = null;
    File file1 = null;
    File file2 = null;
    File file3 = null;
    File file4 = null;

    try {
      file0 = File.createTempFile("copy0-", ".tmp");
      file1 = File.createTempFile("copy1-", ".tmp");
      file2 = File.createTempFile("copy2-", ".tmp");
      file3 = File.createTempFile("copy3-", ".tmp");
      file4 = File.createTempFile("copy4-", ".tmp");
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    Streamlet<Integer> integers = builder.newSource(() -> {
      if (throttle) {
        StreamletUtils.sleep(msDelay, nsDelay);
      }
      return ThreadLocalRandom.current()
          .nextInt(1, 100); });

    List<Streamlet<Integer>> copies = integers.clone(5);

    copies.get(0).setName("copy-0").toSink(new FilesystemSink<>(file0));
    copies.get(1).setName("copy-1").toSink(new FilesystemSink<>(file1));
    copies.get(2).setName("copy-2").toSink(new FilesystemSink<>(file2));
    copies.get(3).setName("copy-3").toSink(new FilesystemSink<>(file3));
    copies.get(4).setName("copy-4").toSink(new FilesystemSink<>(file4));
  }
}