package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.DeleteWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.GetWorkGroupRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static software.amazon.athena.workgroup.HandlerUtils.translateAthenaException;

public class DeleteHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;
  private Logger logger;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest (
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    clientProxy = proxy;
    athenaClient = AthenaClient.create();
    this.logger = logger;

    ResourceModel model = request.getDesiredResourceState();
    return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
        .then(progress -> ensureResourceExists(progress, model))
        .then(progress -> deleteResource(model))
        .onSuccess(progress -> ProgressEvent.<ResourceModel, CallbackContext>builder()
            .status(OperationStatus.SUCCESS)
            .build());
  }

  private ProgressEvent<ResourceModel, CallbackContext> ensureResourceExists(
      ProgressEvent<ResourceModel, CallbackContext> progress, ResourceModel model) {
    final GetWorkGroupRequest getWorkGroupRequest = GetWorkGroupRequest.builder()
        .workGroup(model.getName())
        .build();
    try {
      clientProxy.injectCredentialsAndInvokeV2(getWorkGroupRequest, athenaClient::getWorkGroup);
      return progress;
    } catch (AthenaException e) {
      throw translateAthenaException(e, model.getName());
    }
  }

  private ProgressEvent<ResourceModel, CallbackContext> deleteResource(ResourceModel model) {
    deleteWorkGroup(model);
    logger.log(String.format("%s [%s] deleted successfully",
      ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString()));
    return ProgressEvent.defaultSuccessHandler(model);
  }

  private void deleteWorkGroup(final ResourceModel model) {
    final DeleteWorkGroupRequest deleteWorkGroupRequest = DeleteWorkGroupRequest.builder()
      .workGroup(model.getName())
      .recursiveDeleteOption(model.getRecursiveDeleteOption())
      .build();
    try {
      clientProxy.injectCredentialsAndInvokeV2(deleteWorkGroupRequest, athenaClient::deleteWorkGroup);
    } catch (AthenaException e) {
      throw translateAthenaException(e, model.getName());
    }
  }
}
