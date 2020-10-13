package software.amazon.athena.namedquery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static software.amazon.athena.namedquery.CreateHandler.MAX_NAME_LENGTH;

import software.amazon.awssdk.services.athena.model.CreateNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.CreateNamedQueryResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;

@ExtendWith(MockitoExtension.class)
class CreateHandlerTest {
    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private Logger logger;

    @Test
    void testSuccessState() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .name("name")
                .database("database")
                .queryString("queryString")
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();
        final String namedQueryId = "namedQueryId";

        // Mock
        doReturn(CreateNamedQueryResponse.builder().namedQueryId(namedQueryId).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response
                        = new CreateHandler().handleRequest(proxy, request, null, logger);

        // Assert
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getNamedQueryId()).isEqualTo(namedQueryId);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    void testSuccessStateWithoutName() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
            .database("database")
            .queryString("queryString")
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(resourceModel)
            .clientRequestToken("token")
            .logicalResourceIdentifier("logicalId")
            .build();
        final String namedQueryId = "namedQueryId";

        // Mock
        doReturn(CreateNamedQueryResponse.builder().namedQueryId(namedQueryId).build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response
            = new CreateHandler().handleRequest(proxy, request, null, logger);

        // Assert
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getNamedQueryId()).isEqualTo(namedQueryId);
        assertThat(response.getResourceModel().getName()).isEqualTo(
            IdentifierUtils.generateResourceIdentifier(
                request.getLogicalResourceIdentifier(),
                request.getClientRequestToken(),
                MAX_NAME_LENGTH
            )
        );
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    void testSuccessStateWithWorkGroup() {
        // Prepare inputs
        final String name = "name";
        final String database = "database";
        final String description = "description";
        final String queryString = "queryString";
        final String workGroup = "myWorkGroup";
        final ResourceModel resourceModel = ResourceModel.builder()
                .name(name)
                .database(database)
                .description(description)
                .queryString(queryString)
                .workGroup(workGroup)
                .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(resourceModel)
                .build();
        final String namedQueryId = "namedQueryId";

        // Mock
        final ArgumentCaptor<CreateNamedQueryRequest> requestCaptor = ArgumentCaptor
                .forClass(CreateNamedQueryRequest.class);
        doReturn(CreateNamedQueryResponse.builder().namedQueryId(namedQueryId).build())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        final ProgressEvent<ResourceModel, CallbackContext> response
                = new CreateHandler().handleRequest(proxy, request, null, logger);
        verify(proxy).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
        final CreateNamedQueryRequest argument = requestCaptor.getValue();

        // Assert
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel().getNamedQueryId()).isEqualTo(namedQueryId);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertEquals(name, argument.name());
        assertEquals(database, argument.database());
        assertEquals(description, argument.description());
        assertEquals(queryString, argument.queryString());
        assertEquals(workGroup, argument.workGroup());
    }

    @Test
    void testInternalServerException() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .name("name")
                .database("database")
                .queryString("queryString")
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
                new CreateHandler().handleRequest(proxy, request, null, logger));
    }

    @Test
    void testInvalidRequestException() {
        // Prepare inputs
        final ResourceModel resourceModel = ResourceModel.builder()
                .name("name")
                .database("database")
                .queryString("queryString")
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
                new CreateHandler().handleRequest(proxy, request, null, logger));
    }

}
