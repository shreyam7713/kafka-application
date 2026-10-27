import com.fasterxml.jackson.databind.ObjectMapper;
import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.loganalytics.LogAnalyticsClient;
import com.oracle.bmc.loganalytics.requests.UploadLogEventsFileRequest;
import com.oracle.bmc.loganalytics.model.PayloadType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.ByteArrayInputStream;
import java.util.UUID;
import com.oracle.bmc.retrier.RetryConfiguration;
import java.io.File;

class OciUploadProbe {
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
        var mapper = new ObjectMapper();
        var root = mapper.createObjectNode();
        var event = root.putArray("logEvents").addObject();
        event.put("logSourceName", config.get("oci.la.log.source.name").asText());
        for (String field : new String[]{"logPath", "entityId", "entityType", "timezone"}) {
          String key = switch(field) {
            case "logPath" -> "oci.la.log.path";
            case "entityId" -> "oci.la.entity.id";
            case "entityType" -> "oci.la.entity.type";
            default -> "oci.la.timezone";
          };
          if (config.hasNonNull(key) && !config.get(key).asText().isBlank()) event.put(field,config.get(key).asText());
        }
        String record;
        try(var lines=Files.lines(Path.of(args[1]))) { record=lines.findFirst().orElseThrow(); }
        event.putArray("logRecords").add(record);
        var builder = UploadLogEventsFileRequest.builder()
            .namespaceName(config.get("oci.namespace").asText())
            .logGroupId(config.get("oci.la.log.group.id").asText())
            .payloadType(PayloadType.Json).contentType("application/octet-stream")
            .opcRequestId("config-recheck-"+UUID.randomUUID())
            .retryConfiguration(RetryConfiguration.NO_RETRY_CONFIGURATION)
            .uploadLogEventsFileDetails(new ByteArrayInputStream(mapper.writeValueAsBytes(root)));
        if(config.hasNonNull("oci.la.log.set") && !config.get("oci.la.log.set").asText().isBlank()) builder.logSet(config.get("oci.la.log.set").asText());
        var response = client.uploadLogEventsFile(builder.build());
        System.out.println("Upload HTTP status: " + response.get__httpStatusCode__());
        System.out.println("OCI request ID: " + response.getOpcRequestId());
      } catch (Exception ex) {
        for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
          System.out.println(cause.getClass().getName() + ": " + cause.getMessage());
        }
        System.exit(1);
      }
    }
  }
}
