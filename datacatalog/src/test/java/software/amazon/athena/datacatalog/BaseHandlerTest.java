package software.amazon.athena.datacatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public abstract class BaseHandlerTest {

  protected static final Credentials MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
  protected static final LoggerProxy logger = new LoggerProxy();

  protected abstract BaseHandlerAthena getHandlerInstance();

  @Mock
  protected AmazonWebServicesClientProxy proxy;

  protected ProxyClient<AthenaClient> proxyClient;

  @Mock
  protected AthenaClient athenaClient;

  @BeforeEach
  public void setup() {
    athenaClient = mock(AthenaClient.class);
    proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
    proxyClient = new ProxyClient<AthenaClient>() {
      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
      ResponseT
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
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>> IterableT injectCredentialsAndInvokeIterableV2(
          RequestT request, Function<RequestT, IterableT> requestFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT> injectCredentialsAndInvokeV2InputStream(
          RequestT request, Function<RequestT, ResponseInputStream<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT> injectCredentialsAndInvokeV2Bytes(
          RequestT request, Function<RequestT, ResponseBytes<ResponseT>> requestFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public AthenaClient client() {
        return athenaClient;
      }
    };
  }

  protected ProgressEvent<ResourceModel, CallbackContext> testHandleRequest(ResourceHandlerRequest<ResourceModel> request) {
    return getHandlerInstance().handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
  }

  protected static ResourceModel buildSimpleTestResourceModel() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .type("HIVE")
        .build();
  }

  protected static ResourceModel buildSimpleTestResourceModel2() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .type("GLUE")
        .build();
  }

  protected static ResourceModel buildTestResourceModel() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .description("full description")
        .type("HIVE")
        .tags(Lists.list(
            Tag.builder().key("testKey1").value("someValue1").build()))
        .parameters(ImmutableMap.of("metadata-function", "testing"))
        .build();
  }

  protected static ResourceModel buildTestResourceModelWithNullTags() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .description("full description")
        .type("HIVE")
        .tags(null)
        .parameters(ImmutableMap.of("metadata-function", "testing"))
        .build();
  }

  protected static ResourceModel buildTestResourceModelWithUpdatedTagValue() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .description("full description")
        .type("HIVE")
        .tags(Lists.list(
            Tag.builder().key("testKey1").value("updatedValue").build()))
        .parameters(ImmutableMap.of("metadata-function", "testing"))
        .build();
  }

  protected static ResourceModel buildTestResourceModelWithUpdatedParameters() {
    return ResourceModel.builder()
        .name("TestCatalog")
        .description("full description")
        .type("HIVE")
        .tags(Lists.list(
            Tag.builder().key("testKey1").value("someValue1").build()))
        .parameters(ImmutableMap.of("metadata-function", "testing",
            "parameter2", "value2"))
        .build();
  }

  protected void assertSuccessState(ProgressEvent<ResourceModel, CallbackContext> response) {
    assertThat(response).isNotNull();
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }
}
