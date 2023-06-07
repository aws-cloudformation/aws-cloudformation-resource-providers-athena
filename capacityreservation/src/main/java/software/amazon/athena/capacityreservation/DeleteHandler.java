package software.amazon.athena.capacityreservation;


import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class DeleteHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> processModelIdentifier(progress))
                .then(progress -> cancelCapacityReservation(proxy, proxyClient, progress))
                .then(progress -> deleteCapacityReservation(proxy, proxyClient, progress))
                .then(progress -> ProgressEvent.defaultSuccessHandler(null));
    }

    private ProgressEvent<ResourceModel, CallbackContext> cancelCapacityReservation(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("AWS-Athena-CapacityReservation::Cancel", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::translateToCancelCapacityReservationRequest)
                .makeServiceCall((cancelCapacityReservationRequest, athenaClientProxyClient) -> athenaClientProxyClient.injectCredentialsAndInvokeV2(cancelCapacityReservationRequest,
                        athenaClientProxyClient.client()::cancelCapacityReservation))
                .stabilize((awsRequest, awsResponse, athenaClientProxyClient, model, context) -> {
                    GetCapacityReservationRequest reservationRequest =
                            Translator.translateToGetCapacityReservationRequest(model);
                    GetCapacityReservationResponse reservationResponse =
                            athenaClientProxyClient.injectCredentialsAndInvokeV2(reservationRequest,
                                    athenaClientProxyClient.client()::getCapacityReservation);
                    logger.log(String.format("%s [%s] status: %s", ResourceModel.TYPE_NAME, model.getName(),
                            reservationResponse.capacityReservation().status()));
                    return reservationResponse.capacityReservation().status().equals(CapacityReservationStatus.CANCELLED);
                })
                .handleError((cancelCapacityReservationRequest, e, athenaClientProxyClient, resourceModel, context) -> {
                    // If reservation already cancelled, continue with delete
                    if (isAlreadyCancelledException(e)) {
                        logger.log(String.format("%s [%s] already cancelled, proceeding to delete", ResourceModel.TYPE_NAME, resourceModel.getName()));
                        return ProgressEvent.progress(resourceModel, context);
                    }
                    if (isNotFoundException(e)) {
                        return ProgressEvent.failed(null,
                                context,
                                HandlerErrorCode.NotFound,
                                e.getMessage());
                    }
                    return ProgressEvent.failed(null,
                            context,
                            HandlerErrorCode.GeneralServiceException,
                            e.getMessage());
                })
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> deleteCapacityReservation(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progress) {
        return proxy.initiate("AWS-Athena-CapacityReservation::Delete", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                .translateToServiceRequest(Translator::translateToDeleteCapacityReservationRequest)
                .makeServiceCall((deleteCapacityReservationRequest, athenaClientProxyClient) ->
                        athenaClientProxyClient.injectCredentialsAndInvokeV2(deleteCapacityReservationRequest,
                                athenaClientProxyClient.client()::deleteCapacityReservation))
                .progress();
    }
}
