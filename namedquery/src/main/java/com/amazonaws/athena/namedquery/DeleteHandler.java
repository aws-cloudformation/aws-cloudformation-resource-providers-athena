package com.amazonaws.athena.namedquery;

import com.amazonaws.cloudformation.proxy.AmazonWebServicesClientProxy;
import com.amazonaws.cloudformation.proxy.Logger;
import com.amazonaws.cloudformation.proxy.OperationStatus;
import com.amazonaws.cloudformation.proxy.ProgressEvent;
import com.amazonaws.cloudformation.proxy.ResourceHandlerRequest;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeleteNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AthenaClient athenaClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest (
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        athenaClient = AthenaClient.create();

        return deleteResource(request.getDesiredResourceState());
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteResource(ResourceModel model) {
        deleteNamedQuery(model);
        return ProgressEvent.<ResourceModel, CallbackContext>builder()
                .resourceModel(model)
                .status(OperationStatus.SUCCESS)
                .build();
    }

    private void deleteNamedQuery(final ResourceModel model) {
        final DeleteNamedQueryRequest deleteNamedQueryRequest = DeleteNamedQueryRequest.builder()
                .namedQueryId(model.getNamedQueryId())
                .build();
        try {
            clientProxy.injectCredentialsAndInvokeV2(deleteNamedQueryRequest, athenaClient::deleteNamedQuery);
        } catch (InternalServerException e) {
            throw new RuntimeException(e);  // Replace with exceptions from https://github.com/aws-cloudformation/aws-cloudformation-rpdk-java-plugin/tree/master/src/main/java/com/amazonaws/cloudformation/exceptions
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);  // ditto as InternalServerException
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}