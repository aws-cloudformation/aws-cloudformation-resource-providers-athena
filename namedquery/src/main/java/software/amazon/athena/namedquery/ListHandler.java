package software.amazon.athena.namedquery;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesRequest;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.athena.namedquery.HandlerUtils.translateAthenaException;

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
                listNamedQueries(request.getDesiredResourceState(), request.getNextToken());
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

    private ListNamedQueriesResponse listNamedQueries(ResourceModel model, final String nextToken) {
        final ListNamedQueriesRequest listNamedQueriesRequest = ListNamedQueriesRequest.builder()
                .nextToken(nextToken)
                .maxResults(50)
                .build();
        try {
            return clientProxy.injectCredentialsAndInvokeV2(
                    listNamedQueriesRequest,
                    athenaClient::listNamedQueries);
        } catch (AthenaException e) {
            throw translateAthenaException(e, model.getNamedQueryId());
        }
    }
}
