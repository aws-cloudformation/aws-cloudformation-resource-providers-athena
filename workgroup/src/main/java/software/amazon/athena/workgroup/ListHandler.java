package software.amazon.athena.workgroup;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListWorkGroupsRequest;
import software.amazon.awssdk.services.athena.model.ListWorkGroupsResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.ArrayList;
import java.util.List;

public class ListHandler extends BaseHandler<CallbackContext> {
  private AmazonWebServicesClientProxy clientProxy;
  private AthenaClient athenaClient;

  private static final int MAX_RESULTS = 50;

  @Override
  public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
    final AmazonWebServicesClientProxy proxy,
    final ResourceHandlerRequest<ResourceModel> request,
    final CallbackContext callbackContext,
    final Logger logger) {

    clientProxy = proxy;
    athenaClient = AthenaClient.create();

    final List<ResourceModel> workGroups = new ArrayList<>();
    final ListWorkGroupsResponse listWorkGroupsResponse = listWorkgroup(request.getNextToken());
    listWorkGroupsResponse.workGroups().forEach(q ->
      workGroups.add(ResourceModel.builder()
        .name(q.name())
        .description(q.description())
        .creationTime((double) q.creationTime().getEpochSecond())
        .state(q.stateAsString())
        .build())
    );

    return ProgressEvent.<ResourceModel, CallbackContext>builder()
      .resourceModels(workGroups)
      .nextToken(listWorkGroupsResponse.nextToken())
      .status(OperationStatus.SUCCESS)
      .build();
  }

  private ListWorkGroupsResponse listWorkgroup(final String nextToken) {
    final ListWorkGroupsRequest listWorkGroupsRequest = ListWorkGroupsRequest.builder()
      .nextToken(nextToken)
      .maxResults(MAX_RESULTS)
      .build();
    try {
      return clientProxy.injectCredentialsAndInvokeV2(listWorkGroupsRequest, athenaClient::listWorkGroups);
    } catch (InternalServerException e) {
      throw new CfnGeneralServiceException("listWorkGroupsRequest", e);
    } catch (InvalidRequestException e) {
      throw new CfnInvalidRequestException(e.getMessage(), e);
    }
  }
}
