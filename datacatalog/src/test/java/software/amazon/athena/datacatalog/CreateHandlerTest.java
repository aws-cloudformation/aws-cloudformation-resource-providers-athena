package software.amazon.athena.datacatalog;

import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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
}
