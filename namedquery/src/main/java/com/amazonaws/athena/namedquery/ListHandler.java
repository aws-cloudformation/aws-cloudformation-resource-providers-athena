package com.amazonaws.athena.namedquery;

import com.amazonaws.cloudformation.exceptions.CfnGeneralServiceException;
import com.amazonaws.cloudformation.exceptions.CfnInvalidRequestException;
import com.amazonaws.cloudformation.proxy.AmazonWebServicesClientProxy;
import com.amazonaws.cloudformation.proxy.Logger;
import com.amazonaws.cloudformation.proxy.ProgressEvent;
import com.amazonaws.cloudformation.proxy.OperationStatus;
import com.amazonaws.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesRequest;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesResponse;

public class ListHandler extends BaseHandler<CallbackContext> {
    private AmazonWebServicesClientProxy clientProxy;
    private AthenaClient athenaClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        clientProxy = proxy;
        athenaClient = AthenaClient.create();

        final List<ResourceModel> namedQueries = new ArrayList<>();
        final ListNamedQueriesResponse listNamedQueriesResponse =
                listNamedQueries(request.getNextToken());
        listNamedQueriesResponse.namedQueryIds().forEach(q ->
                namedQueries.add(ResourceModel.builder()
                        .namedQueryId(q)
                        .build())
        );

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(namedQueries)
            .nextToken(listNamedQueriesResponse.nextToken())
            .status(OperationStatus.SUCCESS)
            .build();
    }

    private ListNamedQueriesResponse listNamedQueries(final String nextToken) {
        final ListNamedQueriesRequest listNamedQueriesRequest = ListNamedQueriesRequest.builder()
                .nextToken(nextToken)
                .maxResults(50)
                .build();
        try {
            return clientProxy.injectCredentialsAndInvokeV2(
                    listNamedQueriesRequest,
                    athenaClient::listNamedQueries);
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("listNamedQueriesRequest", e);
        } catch (InvalidRequestException e) {
            throw new CfnInvalidRequestException(listNamedQueriesRequest.toString(), e);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
