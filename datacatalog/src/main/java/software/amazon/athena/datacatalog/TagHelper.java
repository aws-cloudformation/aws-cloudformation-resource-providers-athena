package software.amazon.athena.datacatalog;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TagHelper {
    /**
     * convertToMap
     *
     * Converts a collection of Tag objects to a tag-name -> tag-value map.
     *
     * Note: Tag objects with null tag values will not be included in the output
     * map.
     *
     * @param tags Collection of tags to convert
     * @return Converted Map of tags
     */
    public static Map<String, String> convertToMap(final Collection<Tag> tags) {
        if (CollectionUtils.isEmpty(tags)) {
            return Collections.emptyMap();
        }
        return tags.stream()
                .filter(tag -> tag.getValue() != null)
                .collect(Collectors.toMap(
                        Tag::getKey,
                        Tag::getValue,
                        (oldValue, newValue) -> newValue,
                        HashMap::new));
    }

    /**
     * shouldUpdateTags
     *
     * Determines whether user defined tags have been changed during update.
     */
    public static boolean shouldUpdateTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> previousTags = getPreviouslyAttachedTags(handlerRequest);
        final Map<String, String> desiredTags = getNewDesiredTags(handlerRequest);
        return ObjectUtils.notEqual(previousTags, desiredTags);
    }

    /**
     * getPreviouslyAttachedTags
     *
     * If stack tags and resource tags are not merged together in Configuration class,
     * we will get previously attached system (with `aws:cloudformation` prefix) and user defined tags from
     * handlerRequest.getPreviousSystemTags() (system tags),
     * handlerRequest.getPreviousResourceTags() (stack tags),
     * handlerRequest.getPreviousResourceState().getTags() (resource tags).
     *
     * System tags are an optional feature. Merge them to your tags if you have enabled them for your resource.
     * System tags can change on resource update if the resource is imported to the stack.
     */
    public static Map<String, String> getPreviouslyAttachedTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> previousTags = new HashMap<>();

        if (handlerRequest.getPreviousSystemTags() != null) {
            previousTags.putAll(handlerRequest.getPreviousSystemTags());
        }

        // get previous stack level tags from handlerRequest
        if (handlerRequest.getPreviousResourceTags() != null) {
            previousTags.putAll(handlerRequest.getPreviousResourceTags());
        }

        ResourceModel previousModel = handlerRequest.getPreviousResourceState();
        previousTags.putAll(TagHelper.convertToMap(previousModel.getTags()));
        return previousTags;
    }

    /**
     * getNewDesiredTags
     *
     * If stack tags and resource tags are not merged together in Configuration class,
     * we will get new desired system (with `aws:cloudformation` prefix) and user defined tags from
     * handlerRequest.getSystemTags() (system tags),
     * handlerRequest.getDesiredResourceTags() (stack tags),
     * handlerRequest.getDesiredResourceState().getTags() (resource tags).
     *
     * System tags are an optional feature. Merge them to your tags if you have enabled them for your resource.
     * System tags can change on resource update if the resource is imported to the stack.
     */
    public static Map<String, String> getNewDesiredTags(final ResourceHandlerRequest<ResourceModel> handlerRequest) {
        final Map<String, String> desiredTags = new HashMap<>();

        // get desired system level tags from handlerRequest
        if (handlerRequest.getSystemTags() != null) {
            desiredTags.putAll(handlerRequest.getSystemTags());
        }

        // get desired stack level tags from handlerRequest
        if (handlerRequest.getDesiredResourceTags() != null) {
            desiredTags.putAll(handlerRequest.getDesiredResourceTags());
        }

        ResourceModel desiredModel = handlerRequest.getDesiredResourceState();
        desiredTags.putAll(TagHelper.convertToMap(desiredModel.getTags()));
        return desiredTags;
    }

    public static Map<String, String> generateTagsToAddAndUpdate(final Map<String, String> previousTags, final Map<String, String> desiredTags) {
        return desiredTags
                .entrySet()
                .stream()
                .filter(e -> !previousTags.containsKey(e.getKey()) || !Objects.equals(previousTags.get(e.getKey()), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Set<String> generateTagsToRemove(final Map<String, String> previousTags, final Map<String, String> desiredTags) {
        final Set<String> desiredTagNames = desiredTags.keySet();
        return previousTags.keySet().stream()
                .filter(tagName -> !desiredTagNames.contains(tagName))
                .collect(Collectors.toSet());
    }

    /**
     * tagResource during update
     *
     * Calls the service:TagResource API.
     */
    protected static ProgressEvent<ResourceModel, CallbackContext> tagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<AthenaClient> serviceClient,
            final ResourceModel resourceModel,
            final ResourceHandlerRequest<ResourceModel> handlerRequest,
            final CallbackContext callbackContext,
            final Map<String, String> addedTags,
            final Logger logger) {
        logger.log(String.format("[UPDATE][IN PROGRESS] Going to add/update tags for resource: %s with AccountId: %s",
                resourceModel.getName(), handlerRequest.getAwsAccountId()));

        return proxy.initiate("AWS-Athena-DataCatalog::TagOps", serviceClient, resourceModel, callbackContext)
                .translateToServiceRequest(model ->
                        Translator.tagResourceRequest(HandlerUtils.getDatacatalogArn(handlerRequest, handlerRequest.getDesiredResourceState().getName()), addedTags))
                .makeServiceCall((request, client) ->
                        proxy.injectCredentialsAndInvokeV2(request, client.client()::tagResource))
                .progress();
    }

    /**
     * untagResource during update
     *
     * Calls the service:UntagResource API.
     */
    protected static ProgressEvent<ResourceModel, CallbackContext> untagResource(
            final AmazonWebServicesClientProxy proxy,
            final ProxyClient<AthenaClient> serviceClient,
            final ResourceModel resourceModel,
            final ResourceHandlerRequest<ResourceModel> handlerRequest,
            final CallbackContext callbackContext,
            final Set<String> removedTags,
            final Logger logger) {
        logger.log(String.format("[UPDATE][IN PROGRESS] Going to remove tags for resource: %s with AccountId: %s",
                resourceModel.getName(), handlerRequest.getAwsAccountId()));
        return proxy.initiate("AWS-Athena-DataCatalog::TagOps", serviceClient, resourceModel, callbackContext)
                .translateToServiceRequest(model ->
                        Translator.untagResourceRequest(HandlerUtils.getDatacatalogArn(handlerRequest, handlerRequest.getDesiredResourceState().getName()), removedTags))
                .makeServiceCall((request, client) ->
                        proxy.injectCredentialsAndInvokeV2(request, client.client()::untagResource))
                .progress();

    }
}
