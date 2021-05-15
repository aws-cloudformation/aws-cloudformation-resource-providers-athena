package software.amazon.athena.preparedstatement;

import software.amazon.awssdk.services.athena.model.ListPreparedStatementsRequest;
import software.amazon.awssdk.services.athena.model.ListPreparedStatementsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ListPreparedStatementsRequest awsRequest =
            Translator.translateToListRequest(request.getDesiredResourceState(), request.getNextToken());

        ListPreparedStatementsResponse awsResponse = proxy.injectCredentialsAndInvokeV2(
            awsRequest, ClientBuilder.getClient()::listPreparedStatements);

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(Translator.translateFromListRequest(awsResponse, awsRequest.workGroup()))
            .nextToken(awsResponse.nextToken())
            .status(OperationStatus.SUCCESS)
            .build();
    }
}
