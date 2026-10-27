import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.loganalytics.LogAnalyticsClient;
import com.oracle.bmc.loganalytics.requests.GetNamespaceRequest;
import com.oracle.bmc.retrier.RetryConfiguration;
import java.io.File;

class OciConnectivity {
  public static void main(String[] args) throws Exception {
    var config = new ObjectMapper().readTree(new File(args[0])).get("config");
    String path = config.get("oci.config.file").asText().replaceFirst("^~", System.getProperty("user.home"));
    var profile = ConfigFileReader.parse(path, config.get("oci.config.profile").asText());
    var options = ClientConfiguration.builder().connectionTimeoutMillis(5000).readTimeoutMillis(10000)
        .retryConfiguration(RetryConfiguration.NO_RETRY_CONFIGURATION).build();
    try (var client = LogAnalyticsClient.builder().configuration(options)
        .build(new ConfigFileAuthenticationDetailsProvider(profile))) {
      client.setRegion(config.get("oci.region").asText());
      System.out.println("Endpoint: " + client.getEndpoint());
      try {
        var response = client.getNamespace(GetNamespaceRequest.builder()
            .namespaceName(config.get("oci.namespace").asText())
            .retryConfiguration(RetryConfiguration.NO_RETRY_CONFIGURATION).build());
        System.out.println("Namespace read HTTP status: " + response.get__httpStatusCode__());
      } catch (Exception ex) {
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
          System.out.println(cause.getClass().getName() + ": " + cause.getMessage());
        }
        System.exit(1);
      }
    }
  }
}
