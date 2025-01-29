package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.getDataCatalog;
import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import software.amazon.awssdk.services.athena.AthenaClient;

import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.DataCatalog;
import software.amazon.awssdk.services.athena.model.DataCatalogStatus;
import software.amazon.awssdk.services.athena.model.DataCatalogType;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
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
        final ProxyClient<AthenaClient> athenaProxyClient,
        final Logger logger) {

        this.logger = logger;

        return proxy.initiate("athena::createDataCatalog", athenaProxyClient,
            request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(
                    model -> Translator.createDataCatalogRequest(model,
                                                                 request.getDesiredResourceTags(),
                                                                 request.getSystemTags()))
            .makeServiceCall(this::createDataCatalog)
            .stabilize((createRequest, createResponse, client, model, context) -> {
                if (model.getType().equals(DataCatalogType.FEDERATED.name())) {
                    return !getDataCatalog(client, model).status().equals(DataCatalogStatus.CREATE_IN_PROGRESS);
                }
                return true;
            })
            .done((createRequest, createResponse, client, model, context) -> {
                OperationStatus operationStatus = OperationStatus.SUCCESS;
                ProgressEvent.ProgressEventBuilder<ResourceModel, CallbackContext> progressEventBuilder = ProgressEvent.<ResourceModel, CallbackContext>builder()
                        .resourceModel(model);
                if (model.getType().equals(DataCatalogType.FEDERATED.name())) {
                    DataCatalog dataCatalog = getDataCatalog(client, model);
                    if (!dataCatalog.status().equals(DataCatalogStatus.CREATE_COMPLETE)) {
                        operationStatus = OperationStatus.FAILED;
                        progressEventBuilder.message(dataCatalog.error());
                    }
                }
                return progressEventBuilder.status(operationStatus).build();
            });
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
