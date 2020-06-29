package software.amazon.athena.datacatalog;

import software.amazon.awssdk.services.athena.model.DeleteDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
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
public class DeleteHandlerTest extends BaseHandlerTest {

    @Override
    protected BaseHandlerAthena getHandlerInstance() {
        return new DeleteHandler();
    }

    protected ProgressEvent<ResourceModel, CallbackContext> testHandleRequest(ResourceHandlerRequest<ResourceModel> request) {
        return getHandlerInstance().handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
    }

    @Test
    public void testSuccessState() {
        final ResourceModel model = buildSimpleTestResourceModel();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();
        // Mock
        when(proxyClient.client().deleteDataCatalog(any(DeleteDataCatalogRequest.class)))
            .thenReturn(DeleteDataCatalogResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertSuccessState(response);
        assertThat(response.getResourceModel()).isNull();
    }

    @Test
    void testExceptionWhenDeleteResourceNotFound() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
            .name("catalog")
            .type("GLUE")
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(resourceModel)
            .build();

        // Mock
        when(proxyClient.client().deleteDataCatalog(any(DeleteDataCatalogRequest.class)))
            .thenThrow(InvalidRequestException.builder()
                .message("Invalid request provided: DataCatalog was not found").build());
        // Call
        assertThrows(CfnNotFoundException.class, () -> testHandleRequest(request));
    }
}
