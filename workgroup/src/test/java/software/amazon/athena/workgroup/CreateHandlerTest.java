package software.amazon.athena.workgroup;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.CreateWorkGroupResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest {

  @Mock
  private AmazonWebServicesClientProxy proxy;

  @Mock
  private Logger logger;

  @Test
  void testSuccessState() {
    // Prepare inputs
    Tag tag = Tag.builder().key("owner").value("jedi").build();

    final ResourceModel resourceModel = ResourceModel.builder()
      .name("Primary")
      .description("Primary workgroup")
      .tags(Lists.newArrayList(tag))
      .workGroupConfiguration(WorkGroupConfiguration.builder()
                                                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                                                    .enforceWorkGroupConfiguration(false)
                                                    .publishCloudWatchMetricsEnabled(true)
                                                    .requesterPaysEnabled(true)
                                                    .resultConfiguration(ResultConfiguration.builder()
                                                                                            .outputLocation("s3://abc/")
                                                                                            .encryptionConfiguration(EncryptionConfiguration.builder()
                                                                                                                     .encryptionOption("SSE_S3")
                                                                                                                     .build())
                                                                                            .build())
                                                    .engineVersion(EngineVersion.builder()
                                                                                .selectedEngineVersion("Athena engine version 1")
                                                                                .build())
                                                   .build())
      .build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(resourceModel)
      .build();

    // Mock
    doReturn(CreateWorkGroupResponse.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
      = new CreateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testWorkGroupWithAdditionalParametersSuccessState() {
    // Prepare inputs
    Tag tag = Tag.builder().key("owner").value("jedi").build();

    final ResourceModel resourceModel = ResourceModel.builder()
            .name("Primary")
            .description("Primary workgroup")
            .tags(Lists.newArrayList(tag))
            .workGroupConfiguration(WorkGroupConfiguration.builder()
                    .bytesScannedCutoffPerQuery(10_000_000_000L)
                    .enforceWorkGroupConfiguration(false)
                    .publishCloudWatchMetricsEnabled(true)
                    .requesterPaysEnabled(true)
                    .resultConfiguration(ResultConfiguration.builder()
                            .outputLocation("s3://abc/")
                            .encryptionConfiguration(EncryptionConfiguration.builder()
                                    .encryptionOption("SSE_S3")
                                    .build())
                            .expectedBucketOwner("123456789012")
                            .aclConfiguration(AclConfiguration.builder().s3AclOption("BUCKET_OWNER_FULL_CONTROL").build())
                            .build())
                    .engineVersion(EngineVersion.builder()
                            .selectedEngineVersion("Athena engine version 1")
                            .build())
                    .additionalConfiguration("{\"additionalConfig\": \"some_config\"}")
                    .executionRole("arn:aws:iam::123456789012:role/service-role/fake-execution-role")
                    .customerContentEncryptionConfiguration(CustomerContentEncryptionConfiguration.builder()
                            .kmsKey("arn:aws:kms:us-east-1:123456789012:key/fake-kms-key-id").build())
                    .build())
            .build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(resourceModel)
            .build();

    // Mock
    doReturn(CreateWorkGroupResponse.builder().build())
            .when(proxy)
            .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
            = new CreateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();

  }

  @Test
  void testSuccessStateWithNullableTags() {
    final ResourceModel resourceModel = ResourceModel.builder()
      .name("Primary")
      .description("Primary workgroup")
      .tags(null)
      .workGroupConfiguration(WorkGroupConfiguration.builder()
        .requesterPaysEnabled(true)
        .resultConfiguration(null)
        .build())
      .build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(resourceModel)
      .build();

    // Mock
    doReturn(CreateWorkGroupResponse.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
      = new CreateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }


  @Test
  void testSuccessStateWithNullableResultConfiguration() {
    // Prepare inputs
    Tag tag = Tag.builder().key("owner").value("jedi").build();

    final ResourceModel resourceModel = ResourceModel.builder()
      .name("Primary")
      .description("Primary workgroup")
      .tags(Lists.newArrayList(tag))
      .workGroupConfiguration(WorkGroupConfiguration.builder()
        .bytesScannedCutoffPerQuery(10_000_000_000L)
        .enforceWorkGroupConfiguration(false)
        .publishCloudWatchMetricsEnabled(true)
        .requesterPaysEnabled(true)
        .resultConfiguration(null)
        .engineVersion(null)
        .build())
      .build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(resourceModel)
      .build();

    // Mock
    doReturn(CreateWorkGroupResponse.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
      = new CreateHandler().handleRequest(proxy, request, null, logger);

    // Assert
    assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
    assertThat(response.getCallbackContext()).isNull();
    assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    assertThat(response.getMessage()).isNull();
    assertThat(response.getErrorCode()).isNull();
  }

  @Test
  void testSuccessStateWithNullableWorkGroupConfiguration() {
    // Prepare inputs
    Tag tag = Tag.builder().key("owner").value("jedi").build();

    final ResourceModel resourceModel = ResourceModel.builder()
      .name("Primary")
      .description("Primary workgroup")
      .tags(Lists.newArrayList(tag))
      .workGroupConfiguration(null)
      .build();
    final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
      .desiredResourceState(resourceModel)
      .build();

    // Mock
    doReturn(CreateWorkGroupResponse.builder().build())
      .when(proxy)
      .injectCredentialsAndInvokeV2(any(), any());

    // Call
    final ProgressEvent<ResourceModel, CallbackContext> response
      = new CreateHandler().handleRequest(proxy, request, null, logger);

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
      new CreateHandler().handleRequest(proxy, request, null, logger));
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
      new CreateHandler().handleRequest(proxy, request, null, logger));
  }

}
