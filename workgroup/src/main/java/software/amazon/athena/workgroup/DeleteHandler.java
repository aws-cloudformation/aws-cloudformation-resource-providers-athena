package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeleteWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandler<CallbackContext> {
  static final String WORKGROUP_NOT_EMPTY_ERROR_MSG = "WORKGROUP_NOT_EMPTY";

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

    return deleteResource(request.getDesiredResourceState());
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
    } catch (InternalServerException e) {
      throw new CfnGeneralServiceException("deleteWorkGroup", e);
    } catch (InvalidRequestException e) {
      if (e.athenaErrorCode().equalsIgnoreCase(WORKGROUP_NOT_EMPTY_ERROR_MSG)) {
        logger.log(String.format("Workgroup [ %s ] not empty", model.getName()));
      }
      throw new CfnInvalidRequestException(e.getMessage(), e);
    }
  }
}
