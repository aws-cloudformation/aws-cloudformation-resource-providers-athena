package software.amazon.athena.capacityreservation;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsRequest;
import software.amazon.awssdk.services.athena.model.ListCapacityReservationsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

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

    @Test
    public void testListRequestHandler() {
        final ListHandler handler = new ListHandler();

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .awsAccountId(ACCOUNT_ID)
                .build();

        int numberOfReservations = 10;
        List<CapacityReservation> reservationList = buildListOfCapacityReservations(numberOfReservations);
        String expectedNextToken = UUID.randomUUID().toString();
        when(sdkClient.listCapacityReservations(any(ListCapacityReservationsRequest.class)))
                .thenReturn(ListCapacityReservationsResponse.builder()
                        .nextToken(expectedNextToken)
                        .capacityReservations(reservationList)
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response =
                handler.handleRequest(proxy, request,  null, proxyClient, logger);

        assertThat(response.getResourceModels().size()).isEqualTo(numberOfReservations);
        response.getResourceModels().stream()
                        .forEach(resource -> assertThat(resource.getArn()).isNotNull());

        assertThat(response.getNextToken()).isEqualTo(expectedNextToken);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    private List<CapacityReservation> buildListOfCapacityReservations(int numberOfReservations) {
        List<CapacityReservation> reservationList = new ArrayList<>();
        for (int i = 0; i < numberOfReservations; i++) {
            reservationList.add(CapacityReservation.builder()
                    .name(CAPACITY_RESERVATION_NAME + i)
                    .targetDpus(i * 4)
                    .build());
        }
        return reservationList;
    }
}
