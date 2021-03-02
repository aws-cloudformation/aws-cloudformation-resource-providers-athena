package software.amazon.athena.workgroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListWorkGroupsResponse;
import software.amazon.awssdk.services.athena.model.WorkGroupSummary;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class ListHandlerTest {

  @Mock
  private AmazonWebServicesClientProxy proxy;

  @Mock
  private Logger logger;

  @Test
  void testSuccessState() {
    // Prepare inputs
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(ResourceModel.builder().build())
      .build();

    EngineVersion engineVersion1 = EngineVersion.builder()
            .selectedEngineVersion("Auto")
            .effectiveEngineVersion("Athena engine version 1").build();
    EngineVersion engineVersion2 = EngineVersion.builder()
            .selectedEngineVersion("Athena engine version 2")
            .effectiveEngineVersion("Athena engine version 2").build();

    WorkGroupSummary workGroup1 = WorkGroupSummary.builder().name("primary").creationTime(Instant.now())
      .state("enabled").engineVersion(engineVersion1).build();
    WorkGroupSummary workGroup2= WorkGroupSummary.builder().name("secondary").creationTime(Instant.now())
      .state("disabled").engineVersion(engineVersion2).build();

    List<WorkGroupSummary> workgroups = new ArrayList<>();
    workgroups.add(workGroup1);
    workgroups.add(workGroup2);

    // Mock
    doReturn(
      ListWorkGroupsResponse.builder()
        .nextToken("nextToken")
        .workGroups(workgroups)
        .build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
      = new ListHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getResourceModels().size()).isEqualTo(workgroups.size());
    assertThat(response.getResourceModels().get(0).getName()).isEqualTo(workgroups.get(0).name());
    assertThat(response.getResourceModels().get(1).getName()).isEqualTo(workgroups.get(1).name());
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testInternalServerException() {
    // Prepare inputs
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(ResourceModel.builder().build())
      .build();

    // Mock
    doThrow(InternalServerException.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnGeneralServiceException.class, () ->
      new ListHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testInvalidRequestException() {
    // Prepare inputs
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(ResourceModel.builder().build())
      .build();

    // Mock
    doThrow(InvalidRequestException.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnInvalidRequestException.class, () ->
      new ListHandler().handleRequest(proxy, request, null, logger));
  }

}
