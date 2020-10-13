package software.amazon.athena.namedquery;

import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        logger.log(String.format("%s [%s] calling dummy update handler",
            ResourceModel.TYPE_NAME, request.getDesiredResourceState().getNamedQueryId()));

        return ProgressEvent.<ResourceModel, CallbackContext>builder()
            .resourceModel(request.getDesiredResourceState())
            .status(OperationStatus.SUCCESS)
            .build();

    }
}
