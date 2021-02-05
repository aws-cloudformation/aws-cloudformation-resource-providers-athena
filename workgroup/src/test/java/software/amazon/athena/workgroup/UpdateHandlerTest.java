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
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
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

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
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

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
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

    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
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
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
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

}
