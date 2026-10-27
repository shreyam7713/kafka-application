import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.sink.SinkRecord;

class WorkerUploadIsolation {
  public static void main(String[] args) throws Exception {
    Path pluginPath = Path.of(args[0]);
    URL[] urls;
    try (Stream<Path> files = Files.walk(pluginPath)) {
      urls = files.filter(p -> p.toString().endsWith(".jar")).sorted().map(p -> {
        try { return p.toUri().toURL(); } catch (Exception ex) { throw new RuntimeException(ex); }
      }).toArray(URL[]::new);
    }
    try (var loader = new PluginClassLoader(pluginPath.toUri().toURL(), urls,
        WorkerUploadIsolation.class.getClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      var json = new ObjectMapper();
      var node = json.readTree(Path.of(args[1]).toFile()).get("config");
      Map<String, String> config = json.convertValue(node, Map.class);
      config.put("oci.config.file", config.get("oci.config.file")
          .replaceFirst("^~", System.getProperty("user.home")));
      String prefix = "com.oracle.observability.kafka.connect.";
      Class<?> cfgClass = loader.loadClass(prefix + "OracleObservabilitySinkConfig");
      Object cfg = cfgClass.getConstructor(Map.class).newInstance(config);
      Class<?> uploaderClass = loader.loadClass(prefix + "OciSdkLogEventsUploader");
      Object uploader = uploaderClass.getConstructor(cfgClass).newInstance(cfg);
      var clientField = uploaderClass.getDeclaredField("client");
      clientField.setAccessible(true);
      Object client = clientField.get(uploader);
      Class<?> api = loader.loadClass("com.oracle.bmc.loganalytics.LogAnalytics");
      var safe = loader.loadClass(prefix + "UploadResponseDiagnostics").getDeclaredMethod("safeText", String.class);
      safe.setAccessible(true);
      clientField.set(uploader, Proxy.newProxyInstance(loader, new Class<?>[]{api}, (proxy, method, values) -> {
        try { return method.invoke(client, values); }
        catch (InvocationTargetException invocation) {
          Throwable failure = invocation.getCause();
          for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            System.out.println("CAUSE " + cause.getClass().getName() + ": " + safe.invoke(null, cause.getMessage()));
            for (int i=0; i<Math.min(cause.getStackTrace().length, 8); i++) {
              System.out.println("  at " + cause.getStackTrace()[i]);
            }
          }
          throw failure;
        }
      }));
      for (String name : List.of("com.oracle.bmc.loganalytics.LogAnalyticsClient",
          "org.glassfish.jersey.client.JerseyClient", "jakarta.ws.rs.client.ClientBuilder",
          "com.fasterxml.jackson.databind.ObjectMapper", "org.apache.http.client.HttpClient")) {
        Class<?> type = loader.loadClass(name);
        System.out.println("CLASS " + name + " loader=" + type.getClassLoader()
            + " source=" + type.getProtectionDomain().getCodeSource().getLocation());
      }
      var chunkerClass = loader.loadClass(prefix + "LogEventsChunker");
      Object chunker = chunkerClass.getConstructor(cfgClass).newInstance(cfg);
      String event = Files.readAllLines(Path.of(args[2])).get(0);
      SinkRecord record = new SinkRecord("la-hackathon-logs", 0, null, null, Schema.STRING_SCHEMA, event, 0);
      List<?> chunks = (List<?>) chunkerClass.getMethod("chunk", Collection.class).invoke(chunker, List.of(record));
      try {
        uploaderClass.getMethod("upload", chunks.get(0).getClass()).invoke(uploader, chunks.get(0));
        System.out.println("EXACT_PLUGIN_UPLOAD_SUCCESS");
      } catch (InvocationTargetException failure) {
        System.out.println("EXACT_PLUGIN_UPLOAD_FAILED: " + failure.getCause());
        System.exit(1);
      } finally {
        uploaderClass.getMethod("close").invoke(uploader);
      }
    }
  }
}
