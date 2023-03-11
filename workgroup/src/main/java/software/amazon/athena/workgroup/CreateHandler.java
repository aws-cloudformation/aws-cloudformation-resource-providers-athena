package software.amazon.athena.workgroup;

import org.apache.commons.collections.CollectionUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.Map;

import static software.amazon.athena.workgroup.HandlerUtils.translateAthenaException;

public class CreateHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Translator translator;
  private Logger logger;
  private ResourceHandlerRequest<ResourceModel> request;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    this.clientProxy = proxy;
    this.athenaClient = AthenaClient.create();
    this.logger = logger;
    this.translator = new Translator();
    this.request = request;

    return createResource(request.getDesiredResourceState());
  }

  private ProgressEvent<ResourceModel, CallbackContext> createResource(ResourceModel model) {
    createWorkgroup();
    logger.log(String.format("%s [%s] created successfully",
      ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString()));
    return ProgressEvent.defaultSuccessHandler(model);
  }

  private CreateWorkGroupResponse createWorkgroup() {
    final ResourceModel model = request.getDesiredResourceState();
    final Map<String, String> stackTags = request.getDesiredResourceTags();
    final Map<String, String> systemTags = request.getSystemTags();
    List<software.amazon.awssdk.services.athena.model.Tag> tags =
            translator.createConsolidatedSdkTagsFromCfnTags(model.getTags(), stackTags, systemTags);

    final CreateWorkGroupRequest createWorkGroupRequest = CreateWorkGroupRequest.builder()
      .name(model.getName())
      .description(model.getDescription())
      .tags(CollectionUtils.isEmpty(tags) ? null: tags)
      .configuration(model.getWorkGroupConfiguration() != null ?
        translator.createSdkWorkgroupConfigurationFromCfnConfiguration(model.getWorkGroupConfiguration()) : null)
      .build();
    try {
      return clientProxy.injectCredentialsAndInvokeV2(createWorkGroupRequest, athenaClient::createWorkGroup);
    } catch (AthenaException e) {
      throw translateAthenaException(e, model.getName());
    }
  }
}
