package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.athena.workgroup.HandlerUtils.translateAthenaException;

public class CreateHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Translator translator;
  private Logger logger;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    clientProxy = proxy;
    athenaClient = AthenaClient.create();
    this.logger = logger;
    this.translator = new Translator();

    final ResourceModel model = request.getDesiredResourceState();

    return createResource(model);
  }

  private ProgressEvent<ResourceModel, CallbackContext> createResource(ResourceModel model) {
    createWorkgroup(model);
    logger.log(String.format("%s [%s] created successfully",
      ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString()));
    return ProgressEvent.defaultSuccessHandler(model);
  }

  private CreateWorkGroupResponse createWorkgroup(final ResourceModel model) {
    final CreateWorkGroupRequest createWorkGroupRequest = CreateWorkGroupRequest.builder()
      .name(model.getName())
      .description(model.getDescription())
      .tags(model.getTags() != null ? translator.createSdkTagsFromCfnTags(model.getTags()) : null)
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
