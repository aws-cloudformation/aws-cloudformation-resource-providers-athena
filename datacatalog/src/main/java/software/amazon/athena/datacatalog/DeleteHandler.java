package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.DeleteDataCatalogResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerAthena {

    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        ProxyClient<AthenaClient> athenaProxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("athena::deleteDataCatalog", athenaProxyClient,
            request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::deleteDataCatalogRequest)
            .makeServiceCall(this::deleteDataCatalog)
            .done(deleteDataCatalogResponse -> ProgressEvent.<ResourceModel, CallbackContext>builder()
                .status(OperationStatus.SUCCESS)
                .build());
    }

    private DeleteDataCatalogResponse deleteDataCatalog(
            final DeleteDataCatalogRequest deleteDataCatalogRequest,
            final ProxyClient<AthenaClient> athenaProxyClient) {
        DeleteDataCatalogResponse response;
        try {
            response = athenaProxyClient.injectCredentialsAndInvokeV2(
                deleteDataCatalogRequest, athenaProxyClient.client()::deleteDataCatalog);
            logger.log(String.format("%s [%s] deleted successfully",
                ResourceModel.TYPE_NAME, deleteDataCatalogRequest.name()));
            return response;
        } catch (AthenaException e) {
            throw handleExceptions(e, deleteDataCatalogRequest.name());
        }
    }
}
