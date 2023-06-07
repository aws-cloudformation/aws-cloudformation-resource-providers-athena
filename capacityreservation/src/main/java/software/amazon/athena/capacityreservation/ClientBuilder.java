package software.amazon.athena.capacityreservation;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.cloudformation.LambdaWrapper;

public class ClientBuilder {
  public static AthenaClient getClient() {
      return AthenaClient.builder()
              .httpClient(LambdaWrapper.HTTP_CLIENT)
              .build();
  }

}
