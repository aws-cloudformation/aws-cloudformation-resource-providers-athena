package software.amazon.athena.capacityreservation;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

import static org.assertj.core.api.Assertions.assertThat;

public class AbstractTestBase {
  protected static final Credentials MOCK_CREDENTIALS;
  protected static final LoggerProxy logger;

  protected static final String ACCOUNT_ID = "123412341234";
  protected static final String AWS_PARTITION = "aws";
  protected static final String AWS_REGION = "us-east-1";
  protected static final String CAPACITY_RESERVATION_NAME = "test_reservation";
  protected static final String WORKGROUP_NAME_1 = "reservation_workgroup_one";
  protected static final String WORKGROUP_NAME_2 = "reservation_workgroup_two";
  protected static final Long TARGET_DPUS = 24L;

  static {
    MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
    logger = new LoggerProxy();
  }
  static ProxyClient<AthenaClient> MOCK_PROXY(
    final AmazonWebServicesClientProxy proxy,
    final AthenaClient sdkClient) {
    return new ProxyClient<AthenaClient>() {
      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
      injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
        return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
      CompletableFuture<ResponseT>
      injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
      IterableT
      injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
        return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
      injectCredentialsAndInvokeV2InputStream(RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
      injectCredentialsAndInvokeV2Bytes(RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {
        throw new UnsupportedOperationException();
      }

      @Override
      public AthenaClient client() {
        return sdkClient;
      }
    };
  }

  protected void assertExpectedTags(Collection<Tag> actualTags, Map<String, String> expectedTags) {
    assertThat(actualTags.size()).isEqualTo(expectedTags.size());
    expectedTags.keySet().stream()
            .forEach(key ->  {
              Optional<Tag> responseTag =
                      actualTags.stream().filter(modelTag -> modelTag.key().equals(key)).findFirst();
              assertThat(responseTag).isPresent();
              assertThat(responseTag.get().value()).isEqualTo(expectedTags.get(key));
            });
  }

  protected void assertExpectedModelTags(Collection<software.amazon.athena.capacityreservation.Tag> actualTags, Map<String, String> expectedTags) {
    assertThat(actualTags.size()).isEqualTo(expectedTags.size());
    expectedTags.keySet().stream()
            .forEach(key ->  {
              Optional<software.amazon.athena.capacityreservation.Tag> responseTag =
                      actualTags.stream().filter(modelTag -> modelTag.getKey().equals(key)).findFirst();
              assertThat(responseTag).isPresent();
              assertThat(responseTag.get().getValue()).isEqualTo(expectedTags.get(key));
            });
  }
}
