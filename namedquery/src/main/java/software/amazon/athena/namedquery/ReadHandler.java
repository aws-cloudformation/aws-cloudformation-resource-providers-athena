package software.amazon.athena.namedquery;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.NamedQuery;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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

        return ProgressEvent.defaultSuccessHandler(getNamedQuery(model));
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
            throw new CfnInvalidRequestException(e.getMessage(), e);
        }
    }
}
