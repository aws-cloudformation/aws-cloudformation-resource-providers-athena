package software.amazon.athena.capacityreservation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CapacityAllocation;
import software.amazon.awssdk.services.athena.model.CapacityAllocationStatus;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.CreateCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.CreateCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationResponse;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

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
    public void testCreateReservationBasic() {
        final CreateHandler handler = new CreateHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation capacityReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS.intValue())
                .status(CapacityReservationStatus.ACTIVE)
                .creationTime(creationTime)
                .allocatedDpus(TARGET_DPUS.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        // Mock create SDK calls
        when(sdkClient.createCapacityReservation(any(CreateCapacityReservationRequest.class)))
                .thenReturn(CreateCapacityReservationResponse.builder().build());
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(capacityReservation)
                        .build());

        // Mock read handler SDK calls
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ArgumentCaptor<CreateCapacityReservationRequest> createReservationCaptor = ArgumentCaptor.forClass(CreateCapacityReservationRequest.class);
        verify(sdkClient, times(1)).createCapacityReservation(createReservationCaptor.capture());
        verify(sdkClient, times(0)).putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testCreateReservationWithAssignment() {
        final CreateHandler handler = new CreateHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .capacityAssignmentConfiguration(CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(Arrays.asList(CapacityAssignment.builder()
                                .workgroupNames(Arrays.asList(WORKGROUP_NAME_1))
                                .build()))
                        .build())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation capacityReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS.intValue())
                .status(CapacityReservationStatus.ACTIVE)
                .creationTime(creationTime)
                .allocatedDpus(TARGET_DPUS.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        // Mock create SDK calls
        when(sdkClient.createCapacityReservation(any(CreateCapacityReservationRequest.class)))
                .thenReturn(CreateCapacityReservationResponse.builder().build());
        when(sdkClient.putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class)))
                .thenReturn(PutCapacityAssignmentConfigurationResponse.builder().build());

        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(capacityReservation)
                        .build());

        // Mock read handler SDK calls
        GetCapacityAssignmentConfigurationResponse getCapacityAssignmentConfigurationResponse = GetCapacityAssignmentConfigurationResponse.builder()
                .capacityAssignmentConfiguration(software.amazon.awssdk.services.athena.model.CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(software.amazon.awssdk.services.athena.model.CapacityAssignment.builder()
                                        .workGroupNames(WORKGROUP_NAME_1)
                                        .build())
                                .build())
                .build();
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenReturn(getCapacityAssignmentConfigurationResponse);
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ArgumentCaptor<CreateCapacityReservationRequest> createReservationCaptor = ArgumentCaptor.forClass(CreateCapacityReservationRequest.class);
        verify(sdkClient, times(1)).createCapacityReservation(createReservationCaptor.capture());
        verify(sdkClient, times(1)).putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testCreateReservationWithTags() {
        final CreateHandler handler = new CreateHandler();

        Map<String, String> userTags = new HashMap<>();
        userTags.put("userTag1", "userValue1");
        userTags.put("userTag2", "userValue2");

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .tags(userTags.keySet().stream()
                        .map(key -> Tag.builder()
                                .key(key)
                                .value(userTags.get(key))
                                .build())
                        .collect(Collectors.toSet()))
                .build();

        Map<String, String> stackTags = new HashMap<>();
        stackTags.put("stackTag1", "stackValue1");
        stackTags.put("stackTag2", "stackValue2");

        Map<String, String> systemTags = new HashMap<>();
        systemTags.put("aws:cloudformation:test1", "systemValue1");
        systemTags.put("aws:cloudformation:test2", "systemValue2");

        Map<String, String> expectedTags = new HashMap<>();
        expectedTags.putAll(stackTags);
        expectedTags.putAll(systemTags);
        expectedTags.putAll(userTags);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .systemTags(systemTags)
                .desiredResourceTags(stackTags)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation capacityReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS.intValue())
                .status(CapacityReservationStatus.ACTIVE)
                .creationTime(creationTime)
                .allocatedDpus(TARGET_DPUS.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        // Mock create SDK calls
        when(sdkClient.createCapacityReservation(any(CreateCapacityReservationRequest.class)))
                .thenReturn(CreateCapacityReservationResponse.builder().build());

        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(capacityReservation)
                        .build());

        // Mock read handler SDK calls
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(expectedTags.keySet().stream()
                                .map(key -> software.amazon.awssdk.services.athena.model.Tag.builder()
                                        .key(key)
                                        .value(expectedTags.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ArgumentCaptor<CreateCapacityReservationRequest> createReservationCaptor = ArgumentCaptor.forClass(CreateCapacityReservationRequest.class);
        verify(sdkClient, times(1)).createCapacityReservation(createReservationCaptor.capture());

        // Verify each of the expected tags was passed in to create call
        CreateCapacityReservationRequest reservationRequest = createReservationCaptor.getValue();
        assertExpectedTags(reservationRequest.tags(), expectedTags);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testCreateReservationPreExistenceFailure() {
        final CreateHandler handler = new CreateHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        // Mock capacity reservation already exists
        when(sdkClient.createCapacityReservation(any(CreateCapacityReservationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource already exists")
                        .build());

        assertThatThrownBy(() -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger)).isInstanceOf(CfnAlreadyExistsException.class);
    }

    @Test
    public void testCreateReservationStabilizationFailure() {
        final CreateHandler handler = new CreateHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation capacityReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS.intValue())
                .status(CapacityReservationStatus.FAILED)
                .creationTime(creationTime)
                .allocatedDpus(TARGET_DPUS.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        // Mock create SDK calls
        when(sdkClient.createCapacityReservation(any(CreateCapacityReservationRequest.class)))
                .thenReturn(CreateCapacityReservationResponse.builder().build());
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(capacityReservation)
                        .build());

        assertThatThrownBy(() -> handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger)).isInstanceOf(CfnNotStabilizedException.class);
    }
}
