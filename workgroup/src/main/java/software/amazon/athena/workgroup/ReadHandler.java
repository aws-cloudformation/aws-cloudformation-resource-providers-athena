package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.WorkGroup;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Translator translator;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    final ResourceModel model = request.getDesiredResourceState();

    clientProxy = proxy;
    athenaClient = AthenaClient.create();
    translator = new Translator();

    return ProgressEvent.defaultSuccessHandler(getWorkGroup(model));
  }

  private ResourceModel getWorkGroup(final ResourceModel model) {
    final GetWorkGroupRequest getWorkGroupRequest = GetWorkGroupRequest.builder()
      .workGroup(model.getPrimaryIdentifier().toString())
      .build();
    try {
      final WorkGroup workGroup = clientProxy.injectCredentialsAndInvokeV2(
        getWorkGroupRequest, athenaClient::getWorkGroup).workGroup();
      return ResourceModel.builder()
        .name(workGroup.name())
        .state(workGroup.stateAsString())
        .description(workGroup.description())
        .creationTime((double) workGroup.creationTime().getEpochSecond())
        .workGroupConfiguration(workGroup.configuration() != null ? translator.createCfnWorkgroupConfigurationFromSdkConfiguration(workGroup.configuration()) : null)
        .build();
    } catch (InternalServerException e) {
      throw new CfnGeneralServiceException("getWorkGroup", e);
    } catch (InvalidRequestException e) {
      throw new CfnInvalidRequestException(e.getMessage(), e);
    }
  }
}
