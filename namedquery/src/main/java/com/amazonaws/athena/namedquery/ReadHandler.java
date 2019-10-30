package com.amazonaws.athena.namedquery;

import com.amazonaws.cloudformation.proxy.AmazonWebServicesClientProxy;
import com.amazonaws.cloudformation.proxy.Logger;
import com.amazonaws.cloudformation.proxy.ProgressEvent;
import com.amazonaws.cloudformation.proxy.OperationStatus;
import com.amazonaws.cloudformation.proxy.ResourceHandlerRequest;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.NamedQuery;

public class ReadHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AthenaClient athenaClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();

        clientProxy = proxy;
        athenaClient = AthenaClient.create();

        final NamedQuery namedQuery = getNamedQuery(model);
        model.setNamedQueryId(namedQuery.namedQueryId());
        model.setName(namedQuery.name());
        model.setDatabase(namedQuery.database());
        model.setDescription(namedQuery.description());
        model.setQueryString(namedQuery.queryString());
        model.setWorkGroup(namedQuery.workGroup());

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModel(model)
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private NamedQuery getNamedQuery(final ResourceModel model) {
        final GetNamedQueryRequest getNamedQueryRequest = GetNamedQueryRequest.builder()
                .namedQueryId(model.getNamedQueryId())
                .build();
        try {
            return clientProxy.injectCredentialsAndInvokeV2(
                    getNamedQueryRequest,
                    athenaClient::getNamedQuery).namedQuery();
        } catch (InternalServerException e) {
            throw new RuntimeException(e);  // Replace with exceptions from https://github.com/aws-cloudformation/aws-cloudformation-rpdk-java-plugin/tree/master/src/main/java/com/amazonaws/cloudformation/exceptions
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);  // ditto as InternalServerException
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
