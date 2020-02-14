package software.amazon.athena.workgroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
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
                                                                  .bytesScannedCutoffPerQuery(100)
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
  void testSuccessStateWithResultConfigurationUpdates() {
    // Prepare inputs
    final ResourceModel resourceModel = ResourceModel.builder()
      .name("Primary")
      .description("Primary workgroup update description")
      .workGroupConfigurationUpdates(WorkGroupConfigurationUpdates.builder()
                                                                  .enforceWorkGroupConfiguration(true)
                                                                  .bytesScannedCutoffPerQuery(100)
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
