package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import software.amazon.awssdk.services.athena.AthenaClient;

import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class CreateHandler extends BaseHandlerAthena {

    private Logger logger;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        ProxyClient<AthenaClient> athenaProxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("athena::createDataCatalog", athenaProxyClient,
            request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::createDataCatalogRequest)
            .makeServiceCall(this::createDataCatalog)
            .success();
    }

    private CreateDataCatalogResponse createDataCatalog(
            final CreateDataCatalogRequest createDataCatalogRequest,
            final ProxyClient<AthenaClient> athenaProxyClient) {
        CreateDataCatalogResponse response;
        try {
            response = athenaProxyClient.injectCredentialsAndInvokeV2(
                createDataCatalogRequest, athenaProxyClient.client()::createDataCatalog);
            logger.log(String.format("%s [%s] created successfully", ResourceModel.TYPE_NAME,
                createDataCatalogRequest.name()));
            return response;
        } catch (AthenaException e) {
            throw handleExceptions(e, createDataCatalogRequest.name());
        }
    }
}
