package software.amazon.athena.namedquery;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DeleteNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.athena.namedquery.HandlerUtils.translateAthenaException;

public class DeleteHandler extends BaseHandler<CallbackContext> {
    static final String QUERY_NOT_FOUND_ERR_MSG = "NAMED_QUERY_NOT_FOUND";

    private AmazonWebServicesClientProxy clientProxy;
    private AthenaClient athenaClient;
    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest (
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final Logger logger) {

        clientProxy = proxy;
        athenaClient = AthenaClient.create();
        this.logger = logger;

        ResourceModel model = request.getDesiredResourceState();
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            .then(progress -> deleteResource(model))
            .onSuccess(progress -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .build());
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteResource(ResourceModel model) {
        deleteNamedQuery(model);
        logger.log(String.format("%s [%s] deleted successfully",
            ResourceModel.TYPE_NAME, model.getNamedQueryId()));
        return ProgressEvent.defaultSuccessHandler(model);
    }

    private void deleteNamedQuery(final ResourceModel model) {
        final DeleteNamedQueryRequest deleteNamedQueryRequest = DeleteNamedQueryRequest.builder()
                .namedQueryId(model.getNamedQueryId())
                .build();
        try {
            clientProxy.injectCredentialsAndInvokeV2(deleteNamedQueryRequest, athenaClient::deleteNamedQuery);
        } catch (AthenaException e) {
            throw translateAthenaException(e, model.getNamedQueryId());
        }
    }
}
