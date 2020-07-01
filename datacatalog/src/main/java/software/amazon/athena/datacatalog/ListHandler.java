package software.amazon.athena.datacatalog;

import java.util.stream.Collectors;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.ListDataCatalogsRequest;
import software.amazon.awssdk.services.athena.model.ListDataCatalogsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ListHandler extends BaseHandlerAthena {

    ProxyClient<AthenaClient> athenaProxyClient;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        athenaProxyClient = proxyClient;
        final ListDataCatalogsResponse listDataCatalogsResponse = proxy.injectCredentialsAndInvokeV2(
            listDataCatalogsRequest(request.getNextToken()), proxyClient.client()::listDataCatalogs
        );

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModels(listDataCatalogsResponse.dataCatalogsSummary()
                .stream().map(Translator::getModelFromDataCatalogSummary)
                .collect(Collectors.toList()))
            .nextToken(listDataCatalogsResponse.nextToken())
            .status(OperationStatus.SUCCESS)
            .callbackContext(callbackContext)
            .build();
    }

    private ListDataCatalogsRequest listDataCatalogsRequest(final String nextToken) {
        return ListDataCatalogsRequest.builder()
            .nextToken(nextToken)
            .maxResults(50)
            .build();
    }
}
