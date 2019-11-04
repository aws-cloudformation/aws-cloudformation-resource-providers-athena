package com.amazonaws.athena.namedquery;

import com.amazonaws.cloudformation.exceptions.CfnGeneralServiceException;
import com.amazonaws.cloudformation.exceptions.CfnInvalidRequestException;
import com.amazonaws.cloudformation.proxy.AmazonWebServicesClientProxy;
import com.amazonaws.cloudformation.proxy.Logger;
import com.amazonaws.cloudformation.proxy.OperationStatus;
import com.amazonaws.cloudformation.proxy.ProgressEvent;
import com.amazonaws.cloudformation.proxy.ResourceHandlerRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import software.amazon.awssdk.services.athena.model.CreateNamedQueryResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;

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

    @Test
    void testThrowable() {
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
        doThrow(new RuntimeException())
                .when(proxy)
                .injectCredentialsAndInvokeV2(any(), any());

        // Call
        assertThrows(RuntimeException.class, () ->
                new CreateHandler().handleRequest(proxy, request, null, logger));
    }

}
