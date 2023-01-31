package software.amazon.athena.workgroup;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.Tag;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.athena.workgroup.HandlerUtils.getWorkGroupArn;
import static software.amazon.athena.workgroup.HandlerUtils.translateAthenaException;

public class UpdateHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Logger logger;
  private Translator translator;
  private ResourceHandlerRequest<ResourceModel> request;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    clientProxy = proxy;
    athenaClient = AthenaClient.create();
    this.request = request;
    this.logger = logger;
    translator = new Translator();

    final ResourceModel model = request.getDesiredResourceState();

    return updateResource(model);
  }

  private ProgressEvent<ResourceModel, CallbackContext> updateResource(ResourceModel model) {
    updateWorkgroup();
    logger.log(String.format("%s [%s] updated successfully",
      ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString()));
    return ProgressEvent.defaultSuccessHandler(model);
  }

  private UpdateWorkGroupResponse updateWorkgroup() {
    final ResourceModel newModel = request.getDesiredResourceState();
    final ResourceModel oldModel = request.getPreviousResourceState();

    final Set<Tag> oldTags = new HashSet<>(translator.createConsolidatedSdkTagsFromCfnTags(oldModel.getTags(), request.getPreviousResourceTags()));
    final Set<Tag> newTags = new HashSet<>(translator.createConsolidatedSdkTagsFromCfnTags(newModel.getTags(), request.getDesiredResourceTags()));
    try {
      // Handle modifications to WorkGroup tags
      if (!oldTags.equals(newTags)) {
        String workGroupARN = getWorkGroupArn(request, newModel.getName());

        // {old tags} - {new tags} = {tags to remove}
        Set<Tag> tagsToRemove = Sets.difference(oldTags, newTags);
        if (!tagsToRemove.isEmpty()) {
          UntagResourceRequest untagResourceRequest = UntagResourceRequest.builder()
            .resourceARN(workGroupARN)
            .tagKeys(tagsToRemove.stream().map(Tag::key).collect(Collectors.toList()))
            .build();
          clientProxy.injectCredentialsAndInvokeV2(untagResourceRequest, athenaClient::untagResource);
        }

        // {new tags} - {old tags} = {tags to add}
        Set<Tag> tagsToAdd = Sets.difference(newTags, oldTags);
        if (!tagsToAdd.isEmpty()) {
          TagResourceRequest tagResourceRequest = TagResourceRequest.builder()
            .resourceARN(workGroupARN)
            .tags(tagsToAdd)
            .build();
          clientProxy.injectCredentialsAndInvokeV2(tagResourceRequest, athenaClient::tagResource);
        }
      }

      // Handle modifications to WorkGroup configuration
      final UpdateWorkGroupRequest.Builder updateRequestBuilder = UpdateWorkGroupRequest.builder()
          .workGroup(newModel.getName())
          .description(newModel.getDescription() != null ? newModel.getDescription() : HandlerUtils.DEFAULT_DESCRIPTION)
          .state(newModel.getState() != null ? newModel.getState() : HandlerUtils.DEFAULT_STATE);

      // Prioritize looking at WorkGroupConfiguration field
      if (newModel.getWorkGroupConfiguration() != null) {
        updateRequestBuilder.configurationUpdates(translator.createSdkConfigurationUpdatesFromCfnConfiguration(newModel.getWorkGroupConfiguration()));
      } else if (newModel.getWorkGroupConfigurationUpdates() != null) {
        updateRequestBuilder.configurationUpdates(translator.createSdkConfigurationUpdatesFromCfnConfigurationUpdates(newModel.getWorkGroupConfigurationUpdates()));
      } else {
        // Both fields are null, apply default WorkGroup settings
        updateRequestBuilder.configurationUpdates(HandlerUtils.getDefaultWorkGroupConfiguration());
      }

      // Submit UpdateWorkGroup request to Athena
      return clientProxy.injectCredentialsAndInvokeV2(updateRequestBuilder.build(), athenaClient::updateWorkGroup);
    } catch (AthenaException e) {
      throw translateAthenaException(e, newModel.getName());
    }
  }
}
