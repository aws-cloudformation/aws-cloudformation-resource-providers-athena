package software.amazon.athena.workgroup;

import com.google.common.collect.Sets;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.awssdk.services.athena.model.Tag;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class UpdateHandler extends BaseHandler<CallbackContext> {

  /**
   * arn:$partition:athena:$region:$AWSAcctID:workgroup/$workgroup-name
   * See https://docs.aws.amazon.com/athena/latest/ug/workgroups-access.html
   * https://docs.aws.amazon.com/IAM/latest/UserGuide/list_amazonathena.html#amazonathena-resources-for-iam-policies
   */
  private static final String WORKGROUP_ARN_FORMAT = "arn:%s:athena:%s:%s:workgroup/%s";

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
    updateWorkgroup(model);
    logger.log(String.format("%s [%s] updated successfully",
      ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString()));
    return ProgressEvent.defaultSuccessHandler(model);
  }

  private UpdateWorkGroupResponse updateWorkgroup(final ResourceModel model) {
    final UpdateWorkGroupRequest updateWorkGroupRequest = UpdateWorkGroupRequest.builder()
      .workGroup(model.getName())
      .description(model.getDescription())
      .state(model.getState())
      .configurationUpdates(model.getWorkGroupConfigurationUpdates() != null ?
        translator.createSdkConfigurationUpdatesFromCfnConfigurationUpdates(model.getWorkGroupConfigurationUpdates()) : null)
      .build();
    final ResourceModel oldModel = request.getPreviousResourceState();
    final Set<Tag> oldTags = (oldModel == null || oldModel.getTags() == null) ?
      new HashSet<>() : new HashSet<>(translator.createSdkTagsFromCfnTags(oldModel.getTags()));
    final Set<Tag> newTags = model.getTags() == null ?
      new HashSet<>() : new HashSet<>(translator.createSdkTagsFromCfnTags(model.getTags()));
    try {
      // Handle modifications to WorkGroup tags
      if (!oldTags.equals(newTags)) {
        final String workGroupARN = String.format(WORKGROUP_ARN_FORMAT,
            getAwsPartitionForRegion(request.getRegion()),
            request.getRegion(),
            request.getAwsAccountId(),
            model.getName());

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
      return clientProxy.injectCredentialsAndInvokeV2(updateWorkGroupRequest, athenaClient::updateWorkGroup);
    } catch (InternalServerException e) {
      throw new CfnGeneralServiceException("updateWorkGroup", e);
    } catch (InvalidRequestException | ResourceNotFoundException e) {
      throw new CfnInvalidRequestException(e.getMessage(), e);
    }
  }

  /**
   * Using this method in place of "request.getAwsPartition()" as this value not being populated by the service.
   */
  private static String getAwsPartitionForRegion(String region) {
    switch (region) {
      case "cn-north-1":
      case "cn-northwest-1": return "aws-cn";
      case "us-gov-west-1":
      case "us-gov-east-1": return "aws-us-gov";
      default: return "aws";
    }
  }
}
