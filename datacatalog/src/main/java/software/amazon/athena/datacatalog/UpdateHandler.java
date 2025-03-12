package software.amazon.athena.datacatalog;

import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import java.util.Set;
import java.util.Map;

import lombok.Setter;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

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
                .then(progress -> updateTags(proxy, athenaProxyClient, progress, request)));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags (
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progressEvent,
            ResourceHandlerRequest<ResourceModel> request) {

        ResourceModel model = progressEvent.getResourceModel();
        CallbackContext callbackContext = progressEvent.getCallbackContext();
        if (!TagHelper.shouldUpdateTags(request)) {
            return ProgressEvent.defaultSuccessHandler(request.getDesiredResourceState());
        } else {
            Map<String, String> previousTags = TagHelper.getPreviouslyAttachedTags(request);
            Map<String, String> desiredTags = TagHelper.getNewDesiredTags(request);

            Map<String, String> addedTags = TagHelper.generateTagsToAddAndUpdate(previousTags, desiredTags);
            Set<String> removedTags = TagHelper.generateTagsToRemove(previousTags, desiredTags);

            return TagHelper.untagResource(proxy, proxyClient, model, request, callbackContext, removedTags, logger)
                    .then(progressEvent1 ->
                            TagHelper.tagResource(proxy, proxyClient, model, request, callbackContext, addedTags, logger))
                    .then(progressEvent2 -> ProgressEvent.defaultSuccessHandler(request.getDesiredResourceState()));
        }
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

}
