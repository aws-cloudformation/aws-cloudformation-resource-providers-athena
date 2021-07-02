package software.amazon.athena.workgroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.EngineVersion;
import software.amazon.awssdk.services.athena.model.GetWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.GetWorkGroupResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroup;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class ReadHandlerTest {
  @Mock
  private AmazonWebServicesClientProxy proxy;

  @Mock
  private Logger logger;

  @Test
  void testSuccessStateWithWorkGroupConfigurationNullable() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder().name("primary workgroup").build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    final WorkGroup workGroup = WorkGroup.builder()
      .name("primary workgroup")
      .description("the primary workgroup")
      .state("enabled")
      .creationTime(Instant.now())
      .build();

    // Mock
    doReturn(ListTagsForResourceResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(ListTagsForResourceRequest.class), any());
    doReturn(GetWorkGroupResponse.builder().workGroup(workGroup).build()).when(proxy).injectCredentialsAndInvokeV2(any(GetWorkGroupRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response = new ReadHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getResourceModel().getName()).isEqualTo(workGroup.name());
    assertThat(response.getResourceModel().getDescription()).isEqualTo(workGroup.description());
    assertThat(response.getResourceModel().getState()).isEqualTo(workGroup.stateAsString());
    assertThat(response.getResourceModel().getCreationTime()).isEqualTo(Long.toString(workGroup.creationTime().getEpochSecond()));
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessState() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder().name("primary workgroup").build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    software.amazon.awssdk.services.athena.model.EngineVersion engineVersion1 = EngineVersion.builder()
            .selectedEngineVersion("Auto")
            .effectiveEngineVersion("Athena engine version 1").build();

    final WorkGroup workGroup = WorkGroup.builder()
      .name("primary workgroup").description("the primary workgroup")
      .state("enabled")
      .creationTime(Instant.now())
      .configuration(WorkGroupConfiguration.builder()
        .enforceWorkGroupConfiguration(true)
        .bytesScannedCutoffPerQuery(10_000_000_000L)
        .requesterPaysEnabled(true)
        .publishCloudWatchMetricsEnabled(true)
        .resultConfiguration(ResultConfiguration.builder()
          .outputLocation("s3://abc/")
          .encryptionConfiguration(EncryptionConfiguration.builder()
            .encryptionOption("SSE_S3")
            .build())
          .build())
        .engineVersion(engineVersion1)
        .build())
      .build();

    // Mock
    doReturn(ListTagsForResourceResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(ListTagsForResourceRequest.class), any());
    doReturn(GetWorkGroupResponse.builder().workGroup(workGroup).build()).when(proxy).injectCredentialsAndInvokeV2(any(GetWorkGroupRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response = new ReadHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getResourceModel().getName()).isEqualTo(workGroup.name());
    assertThat(response.getResourceModel().getDescription()).isEqualTo(workGroup.description());
    assertThat(response.getResourceModel().getState()).isEqualTo(workGroup.stateAsString());
    assertThat(response.getResourceModel().getCreationTime()).isEqualTo(Long.toString(workGroup.creationTime().getEpochSecond()));
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getBytesScannedCutoffPerQuery())
      .isEqualTo(workGroup.configuration().bytesScannedCutoffPerQuery());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getEnforceWorkGroupConfiguration())
      .isEqualTo(workGroup.configuration().enforceWorkGroupConfiguration());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getRequesterPaysEnabled())
      .isEqualTo(workGroup.configuration().requesterPaysEnabled());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getPublishCloudWatchMetricsEnabled())
      .isEqualTo(workGroup.configuration().publishCloudWatchMetricsEnabled());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getResultConfiguration().getOutputLocation())
      .isEqualTo(workGroup.configuration().resultConfiguration().outputLocation());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getResultConfiguration().getEncryptionConfiguration().getEncryptionOption())
      .isEqualTo(workGroup.configuration().resultConfiguration().encryptionConfiguration().encryptionOption().toString());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getEngineVersion().getSelectedEngineVersion())
            .isEqualTo(engineVersion1.selectedEngineVersion());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getEngineVersion().getEffectiveEngineVersion())
            .isEqualTo(engineVersion1.effectiveEngineVersion());
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessStateWithResultConfigurationNullable() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder().name("primary workgroup").build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    final WorkGroup workGroup = WorkGroup.builder()
      .name("primary workgroup").description("the primary workgroup")
      .state("enabled")
      .creationTime(Instant.now())
      .configuration(WorkGroupConfiguration.builder()
        .enforceWorkGroupConfiguration(true)
        .bytesScannedCutoffPerQuery(10_000_000_000L)
        .requesterPaysEnabled(true)
        .publishCloudWatchMetricsEnabled(true)
        .build())
      .build();

    // Mock
    doReturn(ListTagsForResourceResponse.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(ListTagsForResourceRequest.class), any());
    doReturn(GetWorkGroupResponse.builder().workGroup(workGroup).build()).when(proxy).injectCredentialsAndInvokeV2(any(GetWorkGroupRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response = new ReadHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getResourceModel().getName()).isEqualTo(workGroup.name());
    assertThat(response.getResourceModel().getDescription()).isEqualTo(workGroup.description());
    assertThat(response.getResourceModel().getState()).isEqualTo(workGroup.stateAsString());
    assertThat(response.getResourceModel().getCreationTime()).isEqualTo(Long.toString(workGroup.creationTime().getEpochSecond()));
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getBytesScannedCutoffPerQuery())
      .isEqualTo(workGroup.configuration().bytesScannedCutoffPerQuery());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getEnforceWorkGroupConfiguration())
      .isEqualTo(workGroup.configuration().enforceWorkGroupConfiguration());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getRequesterPaysEnabled())
      .isEqualTo(workGroup.configuration().requesterPaysEnabled());
    assertThat(response.getResourceModel().getWorkGroupConfiguration().getPublishCloudWatchMetricsEnabled())
      .isEqualTo(workGroup.configuration().publishCloudWatchMetricsEnabled());
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testInternalServerException() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
      .name("primary").build();
    final ResourceHandlerRequest<ResourceModel> request =
      ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    // Mock
    doThrow(InternalServerException.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnGeneralServiceException.class, () -> new ReadHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testInvalidRequestException() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
      .name("primary").build();
    final ResourceHandlerRequest<ResourceModel> request =
      ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    // Mock
    doThrow(InvalidRequestException.builder().build()).when(proxy).injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnInvalidRequestException.class, () -> new ReadHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testWorkGroupNotFound() {
    // Prepare inputs
    String workGroup = "someWorkGroup";
    final ResourceModel resourceModel = ResourceModel.builder().name(workGroup).build();
    final ResourceHandlerRequest<ResourceModel> request =
        ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    // Mock
    String message = String.format("WorkGroup %s is not found.", workGroup);
    InvalidRequestException invalidRequestException = InvalidRequestException.builder().message(message).build();
    doThrow(invalidRequestException).when(proxy).injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnNotFoundException.class, () -> new ReadHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testWorkGroupAlreadyExists() {
    // Prepare inputs
    String workGroup = "someWorkGroup";
    final ResourceModel resourceModel = ResourceModel.builder().name(workGroup).build();
    final ResourceHandlerRequest<ResourceModel> request =
        ResourceHandlerRequest.<ResourceModel>builder().desiredResourceState(resourceModel).build();

    // Mock
    String message = String.format("WorkGroup %s is already created", workGroup);
    InvalidRequestException invalidRequestException = InvalidRequestException.builder().message(message).build();
    doThrow(invalidRequestException).when(proxy).injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnAlreadyExistsException.class, () -> new ReadHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testReadReturnsTags() {
    // Prepare inputs
    final String tagKey = "tagKey";
    final String tagValue = "tagValue";
    final List<software.amazon.awssdk.services.athena.model.Tag> sdkTags = Collections.singletonList(software.amazon.awssdk.services.athena.model.Tag.builder()
        .key(tagKey)
        .value(tagValue)
        .build());
    final ResourceModel resourceModel = ResourceModel.builder().name("primary").build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
        .desiredResourceState(resourceModel).build();
    final WorkGroup workGroup = WorkGroup.builder()
        .name("primary")
        .creationTime(Instant.now())
        .build();

    // Mock
    doReturn(ListTagsForResourceResponse.builder().tags(sdkTags).build()).when(proxy)
        .injectCredentialsAndInvokeV2(any(ListTagsForResourceRequest.class), any());
    doReturn(GetWorkGroupResponse.builder().workGroup(workGroup).build()).when(proxy)
        .injectCredentialsAndInvokeV2(any(GetWorkGroupRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response = new ReadHandler().handleRequest(proxy, request, null, logger);

    // Assert tags were translated from SDK tags to CFN tags successfully
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getResourceModel().getName()).isEqualTo(workGroup.name());
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
    List<Tag> cfnTags = response.getResourceModel().getTags();
    assertNotNull(cfnTags);
    assertThat(cfnTags.size() == 1);
    Tag cfnTag = cfnTags.get(0);
    assertEquals(tagKey, cfnTag.getKey());
    assertEquals(tagValue, cfnTag.getValue());
  }
}
