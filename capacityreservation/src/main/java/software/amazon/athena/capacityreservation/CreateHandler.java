package software.amazon.athena.capacityreservation;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.CreateCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Map;


public class CreateHandler extends BaseHandlerStd {
    private Logger logger;

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final ProxyClient<AthenaClient> proxyClient,
        final Logger logger) {

        this.logger = logger;
        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
            // Create capacity reservation and wait for it to become active
            .then(progress -> createCapacityReservation(proxy, proxyClient, progress, request))
            // Put capacity assignment configuration
            .then(progress -> putCapacityAssignmentConfiguration(proxy, proxyClient, progress, logger))
            .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> createCapacityReservation(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progressEvent,
            ResourceHandlerRequest<ResourceModel> request) {
        ResourceModel resourceModel = progressEvent.getResourceModel();
        // Set ARN of capacity reservation
        resourceModel.setArn(Translator.translateToCapacityReservationArn(request.getAwsPartition(),
                request.getRegion(),
                request.getAwsAccountId(),
                resourceModel.getName()));
        // Collect all tags for create, including system tags, stack tags, and tags specified by the user
        Map<String, String> tagsForCreate = TagHelper.collectAllTags(resourceModel.getTags(),
                request.getDesiredResourceTags(), request.getSystemTags());
        // Create capacity reservation and wait for status to stabilize to ACTIVE
        return proxy.initiate("AWS-Athena-CapacityReservation::Create", proxyClient, progressEvent.getResourceModel(), progressEvent.getCallbackContext())
                .translateToServiceRequest(model -> Translator.translateToCreateCapacityReservationRequest(model, TagHelper.convertToSet(tagsForCreate)))
                .makeServiceCall((createCapacityReservationRequest, athenaClientProxyClient) -> {
                    try {
                        CreateCapacityReservationResponse response =
                                athenaClientProxyClient.injectCredentialsAndInvokeV2(createCapacityReservationRequest,
                                        athenaClientProxyClient.client()::createCapacityReservation);
                        logger.log(String.format("Successfully created %s [%s]", ResourceModel.TYPE_NAME, resourceModel.getName()));
                        return response;
                    } catch (AwsServiceException ex) {
                        if (isAlreadyExistsException(ex)) {
                            throw new CfnAlreadyExistsException(ResourceModel.TYPE_NAME,
                                    createCapacityReservationRequest.name());
                        }
                        throw ex;
                    }
                })
                .stabilize((awsRequest, awsResponse, athenaClientProxyClient, model, context) -> {
                    GetCapacityReservationRequest reservationRequest =
                            Translator.translateToGetCapacityReservationRequest(model);
                    GetCapacityReservationResponse reservationResponse =
                            athenaClientProxyClient.injectCredentialsAndInvokeV2(reservationRequest,
                                    athenaClientProxyClient.client()::getCapacityReservation);
                    updateModelFromSdkResponse(model, reservationResponse.capacityReservation());
                    logger.log(String.format("%s [%s] status: %s", ResourceModel.TYPE_NAME, model.getName(), model.getStatus()));
                    if (model.getStatus().equals(CapacityReservationStatus.FAILED.toString())) {
                        throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, resourceModel.getName());
                    }
                    return model.getStatus().equals(CapacityReservationStatus.ACTIVE.toString());
                })
                .progress();
    }
}
