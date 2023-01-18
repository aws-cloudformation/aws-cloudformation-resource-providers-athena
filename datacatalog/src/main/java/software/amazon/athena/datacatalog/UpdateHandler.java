package software.amazon.athena.datacatalog;

import com.google.common.collect.Sets;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.cloudformation.proxy.*;

import software.amazon.awssdk.services.athena.model.Tag;

import java.util.*;

import static java.util.stream.Collectors.toList;
import static software.amazon.athena.datacatalog.HandlerUtils.getDatacatalogArn;
import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

public class UpdateHandler extends BaseHandlerAthena {

    private Logger logger;

    @Setter
    private ReadHandler readHandler;

    public UpdateHandler() {
        readHandler = new ReadHandler();
    }

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        ProxyClient<AthenaClient> athenaProxyClient,
        final Logger logger) {
        this.logger = logger;

        final ResourceModel resourceModel = request.getDesiredResourceState();

        return ProgressEvent.progress(resourceModel, callbackContext)
            .then(progressEvent -> {
                ProgressEvent<ResourceModel, CallbackContext> readProgress =
                    readHandler.handleRequest(proxy, request, callbackContext, proxy.newProxy(this::getClient), logger);
                if (readProgress.isFailed()) {
                    readProgress.setResourceModel(null);
                }
                return readProgress;
            })
            .onSuccess(p -> proxy.initiate("athena::updateDataCatalog", athenaProxyClient, resourceModel, callbackContext)
                .translateToServiceRequest(Translator::updateDataCatalogRequest)
                .makeServiceCall(this::updateDataCatalog)
                .progress()
                .then(progress -> updateTags(request, athenaProxyClient)));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags(
            final ResourceHandlerRequest<ResourceModel> request, final ProxyClient<AthenaClient> proxyClient) {

        final ResourceModel newModel = request.getDesiredResourceState();

        final Set<Tag> oldTags = getTagsFromResourceModel(request.getPreviousResourceTags());
        final Set<Tag> newTags = getTagsFromResourceModel(request.getDesiredResourceTags());
        final Set<Tag> tagsToAdd = Sets.difference(newTags, oldTags);
        final Set<Tag> tagsToRemove = Sets.difference(oldTags, newTags);
        boolean areTagsUpdated = !tagsToAdd.isEmpty() || !tagsToRemove.isEmpty();

        if (areTagsUpdated){
            String arn = getDatacatalogArn(request, newModel.getName());
            if (!tagsToRemove.isEmpty()) {
                UntagResourceRequest untagResourceRequest = UntagResourceRequest.builder()
                        .resourceARN(arn)
                        .tagKeys(tagsToRemove.stream().map(Tag::key).collect(toList()))
                        .build();
                proxyClient.injectCredentialsAndInvokeV2(untagResourceRequest,
                        proxyClient.client()::untagResource);
            }

            if (!tagsToAdd.isEmpty()) {
                TagResourceRequest tagResourceRequest = TagResourceRequest.builder()
                        .resourceARN(arn)
                        .tags(tagsToAdd)
                        .build();
                proxyClient.injectCredentialsAndInvokeV2(tagResourceRequest,
                        proxyClient.client()::tagResource);
            }
        }
        return ProgressEvent.defaultSuccessHandler(newModel);
    }

    private UpdateDataCatalogResponse updateDataCatalog(UpdateDataCatalogRequest updateDataCatalogRequest,
            final ProxyClient<AthenaClient> athenaProxyClient) {
        try {
            return athenaProxyClient.injectCredentialsAndInvokeV2(
                updateDataCatalogRequest, athenaProxyClient.client()::updateDataCatalog);
        } catch (AthenaException e) {
            throw handleExceptions(e, updateDataCatalogRequest.name());
        }
    }

    private Set<Tag> getTagsFromResourceModel(
            Map<String, String> resourceTags) {
        return (MapUtils.isEmpty(resourceTags)) ? new HashSet<>()
                : new HashSet<>(Translator.convertToAthenaSdkTags(resourceTags));
    }
}
