package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {


  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Logger logger;
  private Translator translator;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    clientProxy = proxy;
    athenaClient = AthenaClient.create();
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
    try {
      return clientProxy.injectCredentialsAndInvokeV2(updateWorkGroupRequest, athenaClient::updateWorkGroup);
    } catch (InternalServerException e) {
      throw new CfnGeneralServiceException("updateWorkGroup", e);
    } catch (InvalidRequestException e) {
      throw new CfnInvalidRequestException(e.getMessage(), e);
    }
  }


}
