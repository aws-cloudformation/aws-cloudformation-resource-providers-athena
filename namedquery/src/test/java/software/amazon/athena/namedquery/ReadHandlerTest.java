package software.amazon.athena.namedquery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import software.amazon.awssdk.services.athena.model.GetNamedQueryResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.NamedQuery;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
class ReadHandlerTest {
    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @Test
    void testSuccessState() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .namedQueryId("namedQueryId")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();
        final NamedQuery namedQuery = NamedQuery.builder()
                .database("database")
                .description("description")
                .name("name")
                .queryString("querystring")
                .build();

        // Mock
        doReturn(
                GetNamedQueryResponse.builder()
                        .namedQuery(namedQuery)
                        .build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response
                = new ReadHandler().handleRequest(proxy, request, null, logger);

        // Assert
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getDatabase()).isEqualTo(namedQuery.database());
        assertThat(response.getResourceModel().getDescription()).isEqualTo(namedQuery.description());
        assertThat(response.getResourceModel().getName()).isEqualTo(namedQuery.name());
        assertThat(response.getResourceModel().getQueryString()).isEqualTo(namedQuery.queryString());
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    void testInternalServerException() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .namedQueryId("namedQueryId")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();

        // Mock
        doThrow(InternalServerException.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        assertThrows(CfnGeneralServiceException.class, () ->
                new ReadHandler().handleRequest(proxy, request, null, logger));
    }

    @Test
    void testInvalidRequestException() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .namedQueryId("namedQueryId")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();

        // Mock
        doThrow(InvalidRequestException.builder().build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        assertThrows(CfnInvalidRequestException.class, () ->
                new ReadHandler().handleRequest(proxy, request, null, logger));
    }
}
