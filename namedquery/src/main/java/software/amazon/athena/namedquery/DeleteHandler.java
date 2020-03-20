package software.amazon.athena.namedquery;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeleteNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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

        return deleteResource(request.getDesiredResourceState());
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
        } catch (InternalServerException e) {
            throw new CfnGeneralServiceException("deleteNamedQuery", e);
        } catch (InvalidRequestException e) {
            if (e.athenaErrorCode().equalsIgnoreCase(QUERY_NOT_FOUND_ERR_MSG)) {
                logger.log(String.format("Query with id [ %s ] not found", model.getNamedQueryId()));
                throw new CfnNotFoundException("AWS::Athena::NamedQuery", model.getNamedQueryId());
            }
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }
    }
}
