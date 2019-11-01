package com.amazonaws.athena.namedquery;

import com.amazonaws.cloudformation.exceptions.CfnGeneralServiceException;
import com.amazonaws.cloudformation.exceptions.CfnInvalidRequestException;
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

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModel(getNamedQuery(model))
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private ResourceModel getNamedQuery(final ResourceModel model) {
        final GetNamedQueryRequest getNamedQueryRequest = GetNamedQueryRequest.builder()
                .namedQueryId(model.getNamedQueryId())
                .build();
        try {
            final NamedQuery namedQuery = clientProxy.injectCredentialsAndInvokeV2(
                    getNamedQueryRequest,
                    athenaClient::getNamedQuery).namedQuery();
            return ResourceModel.builder()
                    .namedQueryId(namedQuery.namedQueryId())
                    .name(namedQuery.name())
                    .database(namedQuery.database())
                    .description(namedQuery.description())
                    .queryString(namedQuery.queryString())
                    .build();
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("getNamedQuery", e);
        } catch (InvalidRequestException e) {
            throw new CfnInvalidRequestException(getNamedQueryRequest.toString(), e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
