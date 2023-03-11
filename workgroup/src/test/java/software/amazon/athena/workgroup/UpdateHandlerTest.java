package software.amazon.athena.workgroup;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.AthenaRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.TagResourceResponse;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceResponse;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest {
  @Mock
  private AmazonWebServicesClientProxy proxy;

  @Mock
  private Logger logger;

  @Test
  void testSuccessState() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .state("disabled")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessStateWithWorkGroupConfigurationUpdates() {
    // Prepare inputs
    final EngineVersion engineVersion = EngineVersion.builder()
            .selectedEngineVersion("AUTO")
            .build();

    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .enforceWorkGroupConfiguration(true)
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .requesterPaysEnabled(true)
                    .engineVersion(engineVersion)
                    .build())
            .state("disabled")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessStateWithWorkGroupConfigurationRevertToDefaults() {
    // Prepare inputs
    final software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates defaultUpdates = HandlerUtils.getDefaultWorkGroupConfiguration();
    final String workgroupName = "primary";
    final String description = "Primary workgroup description";
    final ResourceModel oldModel = ResourceModel.builder()
            .name(workgroupName)
            .description(description)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .enforceWorkGroupConfiguration(false)
                    .publishCloudWatchMetricsEnabled(false)
                    .requesterPaysEnabled(true)
                    .build())
            .state("DISABLED")
            .build();
    final ResourceModel newModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .resultConfiguration(ResultConfiguration.builder()
                            .outputLocation("s3://abc/")
                            .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                                    .encryptionOption("SSE_S3")
                                    .build())
                            .expectedBucketOwner("123456789012")
                            .aclConfiguration(AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                            .build())
                    .additionalConfiguration("{\"additionalConfig\": \"some_config\"}")
                    .executionRole("arn:aws:iam::123456789012:role/service-role/fake-execution-role")
                    .customerContentEncryptionConfiguration(CustomerContentEncryptionConfiguration.builder()
                            .kmsKey("arn:aws:kms:us-east-1:123456789012:key/fake-kms-key-id").build())
                    .build())
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(newModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(1)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0) instanceof UpdateWorkGroupRequest);
    UpdateWorkGroupRequest receivedRequest = (UpdateWorkGroupRequest) requests.get(0);

    assertEquals(HandlerUtils.DEFAULT_STATE, receivedRequest.state().toString());
    assertEquals(defaultUpdates.enforceWorkGroupConfiguration(), receivedRequest.configurationUpdates().enforceWorkGroupConfiguration());
    assertEquals(defaultUpdates.publishCloudWatchMetricsEnabled(), receivedRequest.configurationUpdates().publishCloudWatchMetricsEnabled());
    assertEquals(defaultUpdates.requesterPaysEnabled(), receivedRequest.configurationUpdates().requesterPaysEnabled());

    assertTrue(receivedRequest.configurationUpdates().removeBytesScannedCutoffPerQuery());

    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessStateWithBothWorkGroupConfigurationFields() {
    // Provide both WG and WG updates as input and assert it picks the right values
    final software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates defaultUpdates = HandlerUtils.getDefaultWorkGroupConfiguration();
    final String workgroupName = "primary";
    final long oldBytes = 11_111_111_111L;
    final long configBytes = 22_222_222_222L;
    final long configUpdatesBytes = 33_333_333_333L;
    final ResourceModel oldModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(oldBytes)
                    .build())
            .build();
    final ResourceModel newModel = ResourceModel.builder()
          .name(workgroupName)
          .workGroupConfiguration(WorkGroupConfiguration.builder()
              .bytesScannedCutoffPerQuery(configBytes)
              .resultConfiguration(ResultConfiguration.builder()
                      .outputLocation("s3://abc/")
                      .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                              .encryptionOption("SSE_S3")
                              .build())
                      .expectedBucketOwner("123456789012")
                      .aclConfiguration(AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                      .build())
              .build())
          .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
              .bytesScannedCutoffPerQuery(configUpdatesBytes)
              .build())
          .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(newModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(1)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0) instanceof UpdateWorkGroupRequest);
    UpdateWorkGroupRequest receivedRequest = (UpdateWorkGroupRequest) requests.get(0);

    assertEquals(configBytes, receivedRequest.configurationUpdates().bytesScannedCutoffPerQuery());
    assertEquals("123456789012", receivedRequest.configurationUpdates().resultConfigurationUpdates().expectedBucketOwner());

    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

  }

  @Test
  void testSuccessStateWithoutWorkgroupConfigurationUpdatesField() {
    // Provide both WG and WG updates as input and assert it picks the right values
    final software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates defaultUpdates = HandlerUtils.getDefaultWorkGroupConfiguration();
    final String workgroupName = "primary";
    final long oldBytes = 11_111_111_111L;
    final long configBytes = 22_222_222_222L;
    final ResourceModel oldModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(oldBytes)
                    .build())
            .build();
    final ResourceModel newModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(configBytes)
                    .build())
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(newModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(1)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0) instanceof UpdateWorkGroupRequest);
    UpdateWorkGroupRequest receivedRequest = (UpdateWorkGroupRequest) requests.get(0);

    assertEquals(configBytes, receivedRequest.configurationUpdates().bytesScannedCutoffPerQuery());

    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

  }

  @Test
  void testSuccessStateWithoutWorkgroupConfigurationField() {
    // Provide both WG and WG updates as input and assert it picks the right values
    final software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates defaultUpdates = HandlerUtils.getDefaultWorkGroupConfiguration();
    final String workgroupName = "primary";
    final long oldBytes = 11_111_111_111L;
    final long configUpdatesBytes = 33_333_333_333L;
    final ResourceModel oldModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(oldBytes)
                    .build())
            .build();
    final ResourceModel newModel = ResourceModel.builder()
            .name(workgroupName)
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .bytesScannedCutoffPerQuery(configUpdatesBytes)
                    .build())
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(newModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(1)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0) instanceof UpdateWorkGroupRequest);
    UpdateWorkGroupRequest receivedRequest = (UpdateWorkGroupRequest) requests.get(0);

    assertEquals(configUpdatesBytes, receivedRequest.configurationUpdates().bytesScannedCutoffPerQuery());

    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

  }

  @Test
  void testSuccessStateWithAddingTags() {
    // Prepare inputs
    List<Tag> newTags = Lists.list(new Tag("key1", "value1"), new Tag("key2", "value2"));
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .enforceWorkGroupConfiguration(true)
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .requesterPaysEnabled(true)
                    .build())
            .state("disabled")
            .tags(newTags)
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .region("unit-test")
            .awsAccountId("123456789012")
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UpdateWorkGroupRequest.class), any());
    doReturn(TagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(TagResourceRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

    // Verify that tags were registered/unregistered as expected
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(2)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    TagResourceRequest tagRequest = (TagResourceRequest) requests.get(0);
    assertThat(tagRequest.tags().containsAll(newTags));
  }

  @Test
  void testSuccessStateWithAddingSystemTags(){
    // Prepare inputs
    Map<String, String> systemTags = new HashMap<>();
    systemTags.put("aws:tag:systemTagKey", "systemTagValue");
    final ResourceModel resourceModel = ResourceModel.builder()
                                                .name("Primary")
                                                .description("Primary workgroup update description")
                                                .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                                                                                       .enforceWorkGroupConfiguration(true)
                                                                                       .bytesScannedCutoffPerQuery(10_000_000_000L)
                                                                                       .requesterPaysEnabled(true)
                                                                                       .build())
                                                .state("disabled")
                                                .build();
    final ResourceModel oldModel = ResourceModel.builder()
                                           .name("Primary")
                                           .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                  .previousResourceState(oldModel)
                                                                  .desiredResourceState(resourceModel)
                                                                  .systemTags(systemTags)
                                                                  .region("unit-test")
                                                                  .awsAccountId("123456789012")
                                                                  .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UpdateWorkGroupRequest.class), any());
    doReturn(TagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(TagResourceRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

    // Verify that tags were registered/unregistered as expected
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(2)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    TagResourceRequest tagRequest = (TagResourceRequest) requests.get(0);
    assertEquals(tagRequest.tags().size() ,1);
    assertThat(systemTags.containsKey(tagRequest.tags().get(0).key()));
    assertThat(tagRequest.tags().get(0).value().equals(systemTags.get(tagRequest.tags().get(0).key())));
  }

  @Test
  void testSuccessStateWithRemovingSystemTags(){
    // Prepare inputs
    Map<String, String> systemTags = new HashMap<>();
    systemTags.put("aws:tag:systemTagKey", "systemTagValue");
    final ResourceModel resourceModel = ResourceModel.builder()
                                                .name("Primary")
                                                .description("Primary workgroup update description")
                                                .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                                                                                       .enforceWorkGroupConfiguration(true)
                                                                                       .bytesScannedCutoffPerQuery(10_000_000_000L)
                                                                                       .requesterPaysEnabled(true)
                                                                                       .build())
                                                .state("disabled")
                                                .build();
    final ResourceModel oldModel = ResourceModel.builder()
                                           .name("Primary")
                                           .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                  .previousResourceState(oldModel)
                                                                  .desiredResourceState(resourceModel)
                                                                  .previousSystemTags(systemTags)
                                                                  .region("unit-test")
                                                                  .awsAccountId("123456789012")
                                                                  .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UpdateWorkGroupRequest.class), any());
    doReturn(TagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UntagResourceRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

    // Verify that tags were registered/unregistered as expected
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(2)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    assertThat(requests.get(0) instanceof UntagResourceRequest);
    UntagResourceRequest untagRequest = (UntagResourceRequest) requests.get(0);
    assertEquals(untagRequest.tagKeys().size() ,1);
    assertThat(systemTags.containsKey(untagRequest.tagKeys().get(0)));
  }
  @Test
  void testSuccessStateWithRemovingAllTags() {
    // Prepare inputs
    List<Tag> oldTags = Lists.list(new Tag("key1", "value1"), new Tag("key2", "value2"));
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .enforceWorkGroupConfiguration(true)
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .requesterPaysEnabled(true)
                    .build())
            .state("disabled")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .tags(oldTags)
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .region("unit-test")
            .awsAccountId("123456789012")
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UpdateWorkGroupRequest.class), any());
    doReturn(UntagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UntagResourceRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

    // Verify that tags were registered/unregistered as expected
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(2)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    UntagResourceRequest tagRequest = (UntagResourceRequest) requests.get(0);
    assertThat(tagRequest.tagKeys().containsAll(oldTags.stream().map(Tag::getKey).collect(Collectors.toList())));
  }

  @Test
  void testSuccessStateWithNewTagsAndUntagsAndChangedTagValues() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .enforceWorkGroupConfiguration(true)
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .requesterPaysEnabled(true)
                    .build())
            .state("disabled")
            .tags(Lists.list(new Tag("key1", "value1new"), new Tag("key3", "value3")))
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .tags(Lists.list(new Tag("key1", "value1"), new Tag("key2", "value2")))
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .region("unit-test")
            .awsAccountId("123456789012")
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UpdateWorkGroupRequest.class), any());
    doReturn(TagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(TagResourceRequest.class), any());
    doReturn(UntagResourceResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(UntagResourceRequest.class), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

    // Verify that tags were registered/unregistered as expected
    ArgumentCaptor<AthenaRequest> requestCaptor = ArgumentCaptor.forClass(AthenaRequest.class);
    verify(proxy, times(3)).injectCredentialsAndInvokeV2(requestCaptor.capture(), any());
    List<AthenaRequest> requests = requestCaptor.getAllValues();
    UntagResourceRequest untagRequest = (UntagResourceRequest) requests.get(0);
    assertThat(untagRequest.tagKeys().containsAll(Lists.list("key1", "key2")));
    TagResourceRequest tagRequest = (TagResourceRequest) requests.get(1);
    assertThat(tagRequest.tags().containsAll(Lists.list(new Tag("key1", "value1new"), new Tag("key3", "value3"))));
  }

  @Test
  void testSuccessStateWithResultConfigurationUpdates() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup update description")
            .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                    .enforceWorkGroupConfiguration(true)
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .requesterPaysEnabled(true)
                    .removeBytesScannedCutoffPerQuery(true)
                    .resultConfigurationUpdates(ResultConfigurationUpdates.builder()
                            .removeOutputLocation(true)
                            .encryptionConfiguration(EncryptionConfiguration.builder()
                                    .encryptionOption("CSE_KMS")
                                    .kmsKey("eiifcckijivunlgvvggjeiheertfetujenrnndugggnh")
                                    .build())
                            .build())
                    .build())
            .state("disabled")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doReturn(UpdateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new UpdateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }


  @Test
  void testInternalServerException() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doThrow(InternalServerException.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnGeneralServiceException.class, () ->
            new UpdateHandler().handleRequest(proxy, request, null, logger));
  }

  @Test
  void testInvalidRequestException() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup")
            .build();
    final ResourceModel oldModel = ResourceModel.builder()
            .name("Primary")
            .build();

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .previousResourceState(oldModel)
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doThrow(InvalidRequestException.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    assertThrows(CfnInvalidRequestException.class, () ->
            new UpdateHandler().handleRequest(proxy, request, null, logger));
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
}
