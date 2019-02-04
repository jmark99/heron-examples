package com.jmo.streamlets;

import com.jmo.streamlets.utils.StreamletUtils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

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
public class SimpleCloneStreamlet {

  private static final Logger LOG = Logger.getLogger(SimpleCloneStreamlet.class.getName());

  private static int msgTimeout = 30;
  private static boolean addDelay = true;
  private static int msDelay = 0;
  private static int nsDelay = 1;
  private static Config.DeliverySemantics semantics = Config.DeliverySemantics.ATLEAST_ONCE;

  // Default Heron resources to be applied to the topology
  private static final double CPU = 1.5;
  private static final int GIGABYTES_OF_RAM = 8;
  private static final int NUM_CONTAINERS = 2;

  public static void main(String[] args) throws Exception {

    LOG.info(">>> addDelay:     " + addDelay);
    LOG.info(">>> msgTimeout:   " + msgTimeout);
    LOG.info(">>> semantics:    " + semantics);

    SimpleCloneStreamlet streamletInstance = new SimpleCloneStreamlet();
    streamletInstance.runStreamlet(StreamletUtils.getTopologyName(args));
  }

  public void runStreamlet(String topologyName) throws IOException {
    LOG.info(">>> run SimpleCloneStreamlet...");

    Builder builder = Builder.newBuilder();
    createCloneProcessingGraph(builder);

    Config config = Config.newBuilder().setNumContainers(NUM_CONTAINERS)
        .setPerContainerRamInGigabytes(GIGABYTES_OF_RAM).setPerContainerCpu(CPU)
        .setDeliverySemantics(semantics).setUserConfig("topology.message.timeout.secs", msgTimeout)
        .build();

    if (topologyName == null)
      StreamletUtils.runInSimulatorMode((BuilderImpl) builder, config);
    else
      new Runner().run(topologyName, config, builder);
  }

  //
  // Topology specific setup and processing graph creation.
  //

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

  private void createCloneProcessingGraph(Builder builder) throws IOException {

    File file0 = File.createTempFile("copy0-", ".tmp");
    File file1 = File.createTempFile("copy1-", ".tmp");
    File file2 = File.createTempFile("copy2-", ".tmp");
    File file3 = File.createTempFile("copy3-", ".tmp");
    File file4 = File.createTempFile("copy4-", ".tmp");

    Streamlet<Integer> integers = builder.newSource(() -> {
      if (addDelay) {
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