package software.amazon.athena.capacityreservation;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.stream.Collectors;

public abstract class BaseHandlerStd extends BaseHandler<CallbackContext> {
  protected static String NOT_FOUND_ERROR = "not found";
  protected static String ALREADY_EXIST_ERROR = "already exists";
  protected static String NOT_AUTHORIZED_ERROR = "not authorized";
  protected static String ALREADY_CANCELLED_ERROR = "Reservation cannot be modified when state is CANCELLED";

  @Override
  public final ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {
    return handleRequest(
      proxy,
      request,
      callbackContext != null ? callbackContext : new CallbackContext(),
      proxy.newProxy(ClientBuilder::getClient),
      logger
    );
  }

  protected abstract ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final ProxyClient<AthenaClient> proxyClient,
    final Logger logger);

  protected ProgressEvent<ResourceModel, CallbackContext> processModelIdentifier(
          ProgressEvent<ResourceModel, CallbackContext> progressEvent) {
    ResourceModel model = progressEvent.getResourceModel();
    CallbackContext context = progressEvent.getCallbackContext();

    if (model.getName() == null) {
      if (model.getArn() != null) {
        model.setName(Translator.translateArnToCapacityReservationName(model.getArn()));
      } else {
        throw new CfnInvalidRequestException("Request does not contain a valid identifier for " + ResourceModel.TYPE_NAME);
      }
    }
    return ProgressEvent.progress(model, context);

  }
  protected ProgressEvent<ResourceModel, CallbackContext> putCapacityAssignmentConfiguration(
          final AmazonWebServicesClientProxy proxy,
          final ProxyClient<AthenaClient> proxyClient,
          ProgressEvent<ResourceModel, CallbackContext> progress,
          Logger logger) {
    final ResourceModel model = progress.getResourceModel();
    final CallbackContext callbackContext = progress.getCallbackContext();

    if (model.getCapacityAssignmentConfiguration() == null) {
      return ProgressEvent.progress(model, callbackContext);
    }

    logger.log(String.format("Invoking PutCapacityAssignmentConfiguration API :: %s", ResourceModel.TYPE_NAME));
    return proxy.initiate("AWS-Athena-CapacityReservation::PutCapacityAssignmentConfiguration", proxyClient, model, callbackContext)
            .translateToServiceRequest(Translator::translateToPutCapacityAssignmentConfigRequest)
            .makeServiceCall((putCapacityAssignmentConfigurationRequest, athenaClientProxyClient) ->
                    athenaClientProxyClient.injectCredentialsAndInvokeV2(putCapacityAssignmentConfigurationRequest,
                            athenaClientProxyClient.client()::putCapacityAssignmentConfiguration))
            .progress();
  }

  protected ProgressEvent<ResourceModel, CallbackContext> getCapacityReservation(
          AmazonWebServicesClientProxy proxy,
          ProxyClient<AthenaClient> proxyClient,
          ProgressEvent<ResourceModel, CallbackContext> progressEvent,
          ResourceHandlerRequest<ResourceModel> request,
          Logger logger) {
    ResourceModel model = progressEvent.getResourceModel();
    CallbackContext callbackContext = progressEvent.getCallbackContext();
    return proxy.initiate("AWS-Athena-CapacityReservation::GetCapacityReservation", proxyClient, model, callbackContext)
            .translateToServiceRequest(Translator::translateToGetCapacityReservationRequest)
            .makeServiceCall((getCapacityReservationRequest, athenaClientProxyClient) -> athenaClientProxyClient.injectCredentialsAndInvokeV2(getCapacityReservationRequest,
                    athenaClientProxyClient.client()::getCapacityReservation))
            .handleError((reservationRequest, exception, athenaClientProxyClient, resourceModel, context) -> {
              if (exception instanceof InvalidRequestException) {
                InvalidRequestException serviceException = (InvalidRequestException) exception;
                // Return NotFound error is reservation not found
                if (isNotFoundException(serviceException)) {
                  return ProgressEvent.failed(resourceModel,
                          callbackContext,
                          HandlerErrorCode.NotFound,
                          exception.getMessage());
                }
                // Return invalid request for all other InvalidRequestExceptions
                return ProgressEvent.failed(resourceModel,
                        callbackContext,
                        HandlerErrorCode.InvalidRequest,
                        exception.getMessage());
              }
              return ProgressEvent.failed(resourceModel,
                      callbackContext,
                      HandlerErrorCode.GeneralServiceException,
                      exception.getMessage());
            })
            .done(getCapacityReservationResponse -> {
              logger.log(String.format("%s has successfully been read.", ResourceModel.TYPE_NAME));
              model.setArn(Translator.translateToCapacityReservationArn(request.getAwsPartition(),
                      request.getRegion(), request.getAwsAccountId(), model.getName()));
              updateModelFromSdkResponse(model, getCapacityReservationResponse.capacityReservation());
              return ProgressEvent.progress(model, callbackContext);
            });
  }

  protected ProgressEvent<ResourceModel, CallbackContext> getCapacityAssignmentConfiguration(
          AmazonWebServicesClientProxy proxy,
          ProxyClient<AthenaClient> proxyClient,
          ProgressEvent<ResourceModel, CallbackContext> progressEvent,
          Logger logger) {
    ResourceModel model = progressEvent.getResourceModel();
    CallbackContext callbackContext = progressEvent.getCallbackContext();
    return proxy.initiate("AWS-Athena-CapacityReservation::GetCapacityAssignmentConfiguration", proxyClient, model, callbackContext)
            .translateToServiceRequest(Translator::translateToGetCapacityAssignmentConfigRequest)
            .makeServiceCall((getCapacityAssignmentConfigRequest, athenaClientProxyClient) ->
                    athenaClientProxyClient.injectCredentialsAndInvokeV2(getCapacityAssignmentConfigRequest,
                            athenaClientProxyClient.client()::getCapacityAssignmentConfiguration))
            .handleError((athenaRequest, exception, athenaClientProxyClient, resourceModel, context)
                    -> handlePropertyRequestError(exception, resourceModel, context, logger))
            .done(getCapacityAssignmentConfigResponse -> {
              if (getCapacityAssignmentConfigResponse != null) {
                logger.log("CapacityAssignmentConfiguration has successfully been read.");
                model.setCapacityAssignmentConfiguration(
                        Translator.translateToCapacityAssignmentConfiguration(
                                getCapacityAssignmentConfigResponse.capacityAssignmentConfiguration()));
              }
              return ProgressEvent.progress(model, callbackContext);
            });
  }

  protected ProgressEvent<ResourceModel, CallbackContext> getTags(final AmazonWebServicesClientProxy proxy,
                                                                  final ProxyClient<AthenaClient> proxyClient,
                                                                  ProgressEvent<ResourceModel, CallbackContext> progress,
                                                                  Logger logger) {
    ResourceModel model = progress.getResourceModel();
    CallbackContext callbackContext = progress.getCallbackContext();


    return proxy.initiate("AWS-Athena-CapacityReservation::GetCapacityReservationTags", proxyClient, model, callbackContext)
            .translateToServiceRequest(Translator::translateToListTagsForResourceRequest)
            .makeServiceCall((listTagsRequest, athenaClientProxyClient) -> {
              ListTagsForResourceResponse response = athenaClientProxyClient.injectCredentialsAndInvokeV2(listTagsRequest,
                      athenaClientProxyClient.client()::listTagsForResource);
              logger.log(String.format("%s tags have successfully been read.", ResourceModel.TYPE_NAME));
              model.setTags(response.tags()
                      .stream()
                      .map(Translator::translateToTag)
                      .collect(Collectors.toSet()));
              while (response.nextToken() != null) {
                logger.log(String.format("%s tags have a next token, following pagination", ResourceModel.TYPE_NAME));
                ListTagsForResourceRequest nextListRequest =
                        Translator.translateToListTagsForResourceRequest(model,
                                response.nextToken());
                response = athenaClientProxyClient.injectCredentialsAndInvokeV2(nextListRequest,
                        athenaClientProxyClient.client()::listTagsForResource);
                model.getTags().addAll(response.tags().stream()
                        .map(Translator::translateToTag)
                        .collect(Collectors.toList()));
              }
              return response;
            })
            .handleError((athenaRequest, exception, athenaClientProxyClient, resourceModel, context)
                    -> handlePropertyRequestError(exception, resourceModel, context, logger))
            .progress();
  }

  protected ProgressEvent<ResourceModel, CallbackContext> handlePropertyRequestError(
          Exception exception,
          ResourceModel resourceModel,
          CallbackContext callbackContext,
          Logger logger) {
    logger.log("Error: " + exception.getMessage());
    if (exception instanceof AthenaException) {
      AthenaException athenaException = (AthenaException) exception;
      // If fetching property of resource returns not found or not authorized, continue with execution as if property does not exist
      if (isNotFoundException(athenaException)|| isNotAuthorizedException(athenaException)) {
        return ProgressEvent.progress(resourceModel, callbackContext);
      }
    }
    return ProgressEvent.failed(resourceModel,
            callbackContext,
            HandlerErrorCode.GeneralServiceException,
            exception.getMessage());
  }

  protected boolean isNotFoundException(Exception e) {
    return e instanceof InvalidRequestException
            && e.getMessage() != null
            && e.getMessage().contains(NOT_FOUND_ERROR);
  }

  protected boolean isAlreadyExistsException(Exception e) {
    return e instanceof InvalidRequestException
            && e.getMessage() != null
            && e.getMessage().contains(ALREADY_EXIST_ERROR);
  }

  protected boolean isAlreadyCancelledException(Exception e) {
    return e instanceof InvalidRequestException
            && e.getMessage() != null
            && e.getMessage().contains(ALREADY_CANCELLED_ERROR);
  }

  protected boolean isNotAuthorizedException(Exception e) {
    return e instanceof AthenaException
            && e.getMessage() != null
            && e.getMessage().contains(NOT_AUTHORIZED_ERROR);
  }

  protected void updateModelFromSdkResponse(ResourceModel model, CapacityReservation capacityReservation) {
    model.setTargetDpus(capacityReservation.targetDpus().longValue());
    model.setStatus(capacityReservation.statusAsString());
    if (capacityReservation.allocatedDpus() != null) {
      model.setAllocatedDpus(capacityReservation.allocatedDpus().longValue());
    }
    if (capacityReservation.creationTime() != null) {
      model.setCreationTime(Long.toString(capacityReservation.creationTime().getEpochSecond()));
    }
    if (capacityReservation.lastSuccessfulAllocationTime() != null) {
      model.setLastSuccessfulAllocationTime(Long.toString(capacityReservation.lastSuccessfulAllocationTime().getEpochSecond()));
    }
  }
}
