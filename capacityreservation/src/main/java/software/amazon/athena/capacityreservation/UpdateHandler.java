package software.amazon.athena.capacityreservation;

import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CapacityAllocationStatus;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.UpdateCapacityReservationResponse;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class UpdateHandler extends BaseHandlerStd {
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
                // Check capacity reservation exists
                .then(progress ->
                        proxy.initiate("AWS-Athena-CapacityReservation::GetCapacityReservation", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                        .translateToServiceRequest(Translator::translateToGetCapacityReservationRequest)
                        .makeServiceCall((getCapacityReservationRequest, athenaClientProxyClient) -> athenaClientProxyClient.injectCredentialsAndInvokeV2(getCapacityReservationRequest,
                                        athenaClientProxyClient.client()::getCapacityReservation))
                        .handleError((reservationRequest, exception, athenaClientProxyClient, resourceModel, context) -> {
                            if (exception instanceof InvalidRequestException) {
                                InvalidRequestException serviceException = (InvalidRequestException) exception;
                                // Return NotFound error if Athena returns not found
                                if (isNotFoundException(serviceException)) {
                                    return ProgressEvent.failed(resourceModel,
                                            callbackContext,
                                            HandlerErrorCode.NotFound,
                                            exception.getMessage());
                                }
                            }
                            return ProgressEvent.failed(resourceModel,
                                    callbackContext,
                                    HandlerErrorCode.GeneralServiceException,
                                    exception.getMessage());
                        })
                        .done((getCapacityReservationRequest, getCapacityReservationResponse, proxyClient1, resourceModel, context) -> {
                            logger.log(String.format("%s has successfully been read.", ResourceModel.TYPE_NAME));
                            resourceModel.setArn(Translator.translateToCapacityReservationArn(request.getAwsPartition(),
                                    request.getRegion(), request.getAwsAccountId(), resourceModel.getName()));
                            return ProgressEvent.progress(resourceModel, callbackContext);
                        }))
                .then(progress -> updateCapacityReservation(proxy, proxyClient, progress, request))
                .then(progress -> updateCapacityAssignmentConfiguration(proxy, proxyClient, progress, request))
                .then(progress -> updateTags(proxy, proxyClient, progress, request))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateCapacityReservation(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progressEvent,
            ResourceHandlerRequest<ResourceModel> request) {
        ResourceModel previousModel = request.getPreviousResourceState();
        ResourceModel desiredModel = request.getDesiredResourceState();

        // Check if update needed
        if (previousModel.getTargetDpus().equals(desiredModel.getTargetDpus())) {
            return ProgressEvent.progress(progressEvent.getResourceModel(), progressEvent.getCallbackContext());
        }

        ResourceModel model = progressEvent.getResourceModel();
        CallbackContext callbackContext = progressEvent.getCallbackContext();

        return proxy.initiate("AWS-Athena-CapacityReservation::UpdateCapacityReservation", proxyClient, model, callbackContext)
                .translateToServiceRequest(Translator::translateToUpdateCapacityReservationRequest)
                .makeServiceCall((updateCapacityReservationRequest, athenaClientProxyClient) -> {
                        UpdateCapacityReservationResponse updateCapacityReservationResponse =
                            athenaClientProxyClient.injectCredentialsAndInvokeV2(updateCapacityReservationRequest,
                                    athenaClientProxyClient.client()::updateCapacityReservation);
                    logger.log(String.format("Successfully updated %s [%s]", ResourceModel.TYPE_NAME, model.getName()));
                    return updateCapacityReservationResponse;
                })
                // Stabilize when status goes to ACTIVE and last allocation was successful
                .stabilize((awsRequest, awsResponse, athenaClientProxyClient, resourceModel, context) -> {
                    GetCapacityReservationRequest reservationRequest =
                            Translator.translateToGetCapacityReservationRequest(resourceModel);
                    GetCapacityReservationResponse reservationResponse =
                            athenaClientProxyClient.injectCredentialsAndInvokeV2(reservationRequest,
                                    athenaClientProxyClient.client()::getCapacityReservation);
                    updateModelFromSdkResponse(resourceModel, reservationResponse.capacityReservation());
                    logger.log(String.format("%s [%s] status: %s", ResourceModel.TYPE_NAME, model.getName(), model.getStatus()));
                    if (model.getStatus().equals(CapacityReservationStatus.ACTIVE.toString())) {
                        CapacityAllocationStatus allocationStatus = reservationResponse
                                .capacityReservation().lastAllocation().status();
                        if (allocationStatus.equals(CapacityAllocationStatus.SUCCEEDED)) {
                            return true;
                        }
                        // Last allocation did not succeed, fail the update request
                        throw new CfnNotStabilizedException(ResourceModel.TYPE_NAME, resourceModel.getName());
                    }
                    return false;
                })
                .progress();
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateCapacityAssignmentConfiguration(
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progressEvent,
            ResourceHandlerRequest<ResourceModel> request) {
        ResourceModel previousModel = request.getPreviousResourceState();
        ResourceModel desiredModel = request.getDesiredResourceState();

        if (ObjectUtils.notEqual(previousModel.getCapacityAssignmentConfiguration(),
                desiredModel.getCapacityAssignmentConfiguration())) {
            // Removing capacity assignments, set empty capacity assignment configuration
            if (previousModel.getCapacityAssignmentConfiguration() != null
                    && desiredModel.getCapacityAssignmentConfiguration() == null) {
                desiredModel.setCapacityAssignmentConfiguration(CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(Collections.emptyList())
                        .build());
            }
            return putCapacityAssignmentConfiguration(proxy, proxyClient, progressEvent, logger);
        }

        return ProgressEvent.progress(progressEvent.getResourceModel(), progressEvent.getCallbackContext());
    }

    private ProgressEvent<ResourceModel, CallbackContext> updateTags (
            AmazonWebServicesClientProxy proxy,
            ProxyClient<AthenaClient> proxyClient,
            ProgressEvent<ResourceModel, CallbackContext> progressEvent,
            ResourceHandlerRequest<ResourceModel> request) {
        ResourceModel model = progressEvent.getResourceModel();
        CallbackContext callbackContext = progressEvent.getCallbackContext();

        if (!TagHelper.shouldUpdateTags(request)) {
            return ProgressEvent.progress(model, callbackContext);
        }

        Map<String, String> previousTags = TagHelper.getPreviouslyAttachedTags(request);
        Map<String, String> desiredTags = TagHelper.getNewDesiredTags(request);

        Map<String, String> addedTags = TagHelper.generateTagsToAdd(previousTags, desiredTags);
        Set<String> removedTags = TagHelper.generateTagsToRemove(previousTags, desiredTags);

        return TagHelper.untagResource(proxy, proxyClient, model, request, callbackContext, removedTags, logger)
                .then(progressEvent1 ->
                        TagHelper.tagResource(proxy,
                                proxyClient, model, request, callbackContext, addedTags, logger));
    }
}
