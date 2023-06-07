package software.amazon.athena.capacityreservation;

import java.time.Duration;

import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CancelCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.CancelCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.CreateCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.DeleteCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.DeleteCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<AthenaClient> proxyClient;

    @Mock
    AthenaClient sdkClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(AthenaClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @AfterEach
    public void tear_down() {
        verify(sdkClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(sdkClient);
    }

    @Test
    public void testDeleteRequestHandler() {
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        when(sdkClient.cancelCapacityReservation(any(CancelCapacityReservationRequest.class)))
                .thenReturn(CancelCapacityReservationResponse.builder()
                        .build());
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                // Mock first stabilization check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(CapacityReservation.builder()
                                .status(CapacityReservationStatus.CANCELLING)
                                .build())
                        .build())
                // Mock second stabilization check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(CapacityReservation.builder()
                                .status(CapacityReservationStatus.CANCELLED)
                        .build())
                .build());
        when(sdkClient.deleteCapacityReservation(any(DeleteCapacityReservationRequest.class)))
                .thenReturn(DeleteCapacityReservationResponse.builder()
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        //Verify cancel was called with capacity reservation name
        ArgumentCaptor<CancelCapacityReservationRequest> cancelCaptor = ArgumentCaptor.forClass(CancelCapacityReservationRequest.class);
        verify(sdkClient, times(1)).cancelCapacityReservation(cancelCaptor.capture());
        assertThat(cancelCaptor.getValue().name()).isEqualTo(CAPACITY_RESERVATION_NAME);

        //Verify delete was called with capacity reservation name
        ArgumentCaptor<DeleteCapacityReservationRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteCapacityReservationRequest.class);
        verify(sdkClient, times(1)).deleteCapacityReservation(deleteCaptor.capture());
        assertThat(deleteCaptor.getValue().name()).isEqualTo(CAPACITY_RESERVATION_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testDeleteRequestHandlerDoesNotExist() {
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(sdkClient.cancelCapacityReservation(any(CancelCapacityReservationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        verify(sdkClient, times(0)).deleteCapacityReservation(any(DeleteCapacityReservationRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void testDeleteRequestHandlerAlreadyCancelled() {
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(sdkClient.cancelCapacityReservation(any(CancelCapacityReservationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Reservation cannot be modified when state is CANCELLED")
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        //Verify delete was called with capacity reservation name when resource already cancelled
        ArgumentCaptor<DeleteCapacityReservationRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteCapacityReservationRequest.class);
        verify(sdkClient, times(1)).deleteCapacityReservation(deleteCaptor.capture());
        assertThat(deleteCaptor.getValue().name()).isEqualTo(CAPACITY_RESERVATION_NAME);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testDeleteRequestHandlerGeneralServiceException() {
        final DeleteHandler handler = new DeleteHandler();

        final ResourceModel model = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        when(sdkClient.cancelCapacityReservation(any(CancelCapacityReservationRequest.class)))
                .thenThrow(AthenaException.builder()
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.GeneralServiceException);
    }


}
