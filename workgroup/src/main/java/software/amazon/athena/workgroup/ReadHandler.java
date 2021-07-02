package software.amazon.athena.workgroup;

import com.google.common.collect.Lists;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.GetWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.athena.model.Tag;
import software.amazon.awssdk.services.athena.model.WorkGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;

import static software.amazon.athena.workgroup.HandlerUtils.getWorkGroupArn;
import static software.amazon.athena.workgroup.HandlerUtils.translateAthenaException;

public class ReadHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Translator translator;
  private ResourceHandlerRequest<ResourceModel> request;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    final ResourceModel model = request.getDesiredResourceState();

    this.clientProxy = proxy;
    this.athenaClient = AthenaClient.create();
    this.translator = new Translator();
    this.request = request;

    return ProgressEvent.defaultSuccessHandler(getWorkGroup(model));
  }

  private ResourceModel getWorkGroup(final ResourceModel model) {
    final GetWorkGroupRequest getWorkGroupRequest = GetWorkGroupRequest.builder()
      .workGroup(model.getName())
      .build();
    final String workGroupARN = getWorkGroupArn(request, model.getName());
    try {
      // Get WorkGroup
      final WorkGroup workGroup = clientProxy.injectCredentialsAndInvokeV2(getWorkGroupRequest, athenaClient::getWorkGroup).workGroup();

      // List all tags for this WorkGroup
      ListTagsForResourceRequest listTagsRequest = ListTagsForResourceRequest.builder()
          .resourceARN(workGroupARN)
          .maxResults(100)
          .build();
      String nextToken;
      List<Tag> tags = Lists.newArrayList();
      do {
        ListTagsForResourceResponse listTagsResponse = clientProxy.injectCredentialsAndInvokeV2(
            listTagsRequest, athenaClient::listTagsForResource);
        tags.addAll(listTagsResponse.tags());
        nextToken = listTagsResponse.nextToken();
        if (nextToken != null) {
          listTagsRequest = listTagsRequest.toBuilder().nextToken(nextToken).build();
        }
      } while (nextToken != null);

      return ResourceModel.builder()
        .name(workGroup.name())
        .state(workGroup.stateAsString())
        .description(workGroup.description())
        .recursiveDeleteOption(model.getRecursiveDeleteOption())
        .creationTime(Long.toString(workGroup.creationTime().getEpochSecond()))
        .workGroupConfiguration(workGroup.configuration() != null ? translator.createCfnWorkgroupConfigurationFromSdkConfiguration(workGroup.configuration()) : null)
        .workGroupConfigurationUpdates(model.getWorkGroupConfigurationUpdates())
        .tags(translator.createCfnTagsFromSdkTags(tags))
        .build();
    } catch (AthenaException e) {
        throw translateAthenaException(e, model.getName());
    }
  }
}
