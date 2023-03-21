package software.amazon.athena.datacatalog;

import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaRequest;
import software.amazon.awssdk.services.athena.model.AthenaResponse;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends BaseHandlerTest {

    @Override
    protected BaseHandlerAthena getHandlerInstance() {
        return new CreateHandler();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> testHandleRequest(ResourceHandlerRequest<ResourceModel> request) {
        return getHandlerInstance().handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
    }

    @Test
    public void testSuccessState() {
        final ResourceModel model = buildTestResourceModel();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        // Mock
        when(proxyClient.client().createDataCatalog(any(CreateDataCatalogRequest.class)))
        .thenReturn((CreateDataCatalogResponse)
            CreateDataCatalogResponse.builder()
                .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
                .build()
        );

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        // Assert
        assertSuccessState(response);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    void testSuccessStateWithNullableTags() {
        final ResourceModel resourceModel = buildTestResourceModelWithNullTags();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(resourceModel)
            .build();

        // Mock
        when(proxyClient.client().createDataCatalog(any(CreateDataCatalogRequest.class)))
            .thenReturn(CreateDataCatalogResponse.builder().build());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        // Assert
        assertSuccessState(response);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    void testExceptionWhenCreateDuplicate() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
            .name("catalog")
            .type("GLUE")
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(resourceModel)
            .build();

        // Mock
        when(proxyClient.client().createDataCatalog(any(CreateDataCatalogRequest.class)))
            .thenThrow(InvalidRequestException.builder()
                .message("Invalid request provided: DataCatalog has already been created").build());
        // Call
        assertThrows(CfnAlreadyExistsException.class, () -> testHandleRequest(request));
    }

    @Test
    void testSuccessStateWithSystemTags() {
        final ResourceModel resourceModel = buildTestResourceModel();
        Map<String, String> systemTags = new HashMap<>();
        systemTags.put("aws:tag:somekey", "somevalue");
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                    .desiredResourceState(resourceModel)
                                                                    .systemTags(systemTags)
                                                                    .build();
        // Mock
        when(proxyClient.client().createDataCatalog(any(CreateDataCatalogRequest.class)))
              .thenReturn(CreateDataCatalogResponse.builder().build());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

      // Assert
      assertSuccessState(response);
      CreateDataCatalogRequest createDataCatalogRequest =
              captureRequests(verify(proxyClient.client())::createDataCatalog, CreateDataCatalogRequest.class).get(0);
      assertThat(createDataCatalogRequest.tags().containsAll(systemTags.entrySet()));
    }

    private <T extends AthenaRequest, F extends AthenaResponse> List<T> captureRequests(
            Function<T, F> function, Class<T> clazz) {
        ArgumentCaptor<T> requestCaptor = ArgumentCaptor.forClass(clazz);
        function.apply(requestCaptor.capture());
        return requestCaptor.getAllValues();
    }
}
