package software.amazon.athena.namedquery;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeleteNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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
            throw new CfnInvalidRequestException(deleteNamedQueryRequest.toString(), e);
        }
    }
}
