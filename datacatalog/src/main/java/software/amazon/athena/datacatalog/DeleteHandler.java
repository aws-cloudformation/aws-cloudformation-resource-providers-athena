package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.getDataCatalog;
import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DataCatalog;
import software.amazon.awssdk.services.athena.model.DataCatalogStatus;
import software.amazon.awssdk.services.athena.model.DataCatalogType;
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
            .stabilize((deleteRequest, deleteResponse, client, model, context) -> {
                if (model.getType().equals(DataCatalogType.FEDERATED.name())) {
                    return !getDataCatalog(client, model).status().equals(DataCatalogStatus.DELETE_IN_PROGRESS);
                }
                return true;
            })
            .done((deleteRequest, deleteResponse, client, model, context) -> {
                OperationStatus operationStatus = OperationStatus.SUCCESS;
                ProgressEvent.ProgressEventBuilder<ResourceModel, CallbackContext> progressEventBuilder = ProgressEvent.<ResourceModel, CallbackContext>builder();
                if (model.getType().equals(DataCatalogType.FEDERATED.name())) {
                    DataCatalog dataCatalog = getDataCatalog(client, model);
                    if (!dataCatalog.status().equals(DataCatalogStatus.DELETE_COMPLETE)) {
                        operationStatus = OperationStatus.FAILED;
                        progressEventBuilder.message(dataCatalog.error());
                    }
                }
                return progressEventBuilder
                        .status(operationStatus)
                        .build();
            });
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
