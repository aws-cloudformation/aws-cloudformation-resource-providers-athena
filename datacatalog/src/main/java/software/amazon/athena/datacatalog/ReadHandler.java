package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.getDatacatalogArn;
import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;
import static software.amazon.athena.datacatalog.Translator.convertToResourceModelTags;

import com.google.common.collect.Lists;
import java.util.List;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DataCatalog;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerAthena {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        ProxyClient<AthenaClient> athenaProxyClient,
        final Logger logger) {

        return
            proxy.initiate("athena::getDataCatalog", athenaProxyClient,
            request.getDesiredResourceState(), callbackContext)
            .translateToServiceRequest(Translator::getDataCatalogRequest)
            .makeServiceCall(this::getDataCatalog)
            .done((getDataCatalogRequest, getDataCatalogResponse, proxyInvocation, resourceModel, context) -> {
            // get tags

                String arn = getDatacatalogArn(request, resourceModel.getName());
                System.err.println(arn);
                ListTagsForResourceRequest listTagsRequest = ListTagsForResourceRequest.builder()
                    .resourceARN(arn)
                    .maxResults(100)
                    .build();
                String nextToken;
                List<software.amazon.awssdk.services.athena.model.Tag> tags = Lists.newArrayList();
                do {
                    ListTagsForResourceResponse listTagsResponse = athenaProxyClient
                        .injectCredentialsAndInvokeV2(
                            listTagsRequest,
                            athenaProxyClient.client()::listTagsForResource);
                    tags.addAll(listTagsResponse.tags());
                    nextToken = listTagsResponse.nextToken();
                    if (nextToken != null) {
                      listTagsRequest = listTagsRequest.toBuilder().nextToken(nextToken).build();
                    }
                } while (nextToken != null);

                DataCatalog d = getDataCatalogResponse.dataCatalog();

                return ProgressEvent.defaultSuccessHandler(ResourceModel.builder()
                    .name(d.name())
                    .description(d.description())
                    .type(d.type().toString())
                    .parameters(d.parameters())
                    .tags(convertToResourceModelTags(tags))
                    .build());
            });
    }


    private GetDataCatalogResponse getDataCatalog(
        final GetDataCatalogRequest getDataCatalogRequest,
        final ProxyClient<AthenaClient> athenaProxyClient) {
        try {
            return athenaProxyClient.injectCredentialsAndInvokeV2(
                getDataCatalogRequest, athenaProxyClient.client()::getDataCatalog);
        } catch (AthenaException e) {
            throw handleExceptions(e, getDataCatalogRequest.name());
        }
    }
}
