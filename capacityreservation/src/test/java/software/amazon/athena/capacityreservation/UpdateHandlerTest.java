package software.amazon.athena.capacityreservation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.CapacityAllocation;
import software.amazon.awssdk.services.athena.model.CapacityAllocationStatus;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.PutCapacityAssignmentConfigurationResponse;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.TagResourceResponse;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceResponse;
import software.amazon.awssdk.services.athena.model.UpdateCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.UpdateCapacityReservationResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

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
    public void testUpdateReservationBasic() {
        final UpdateHandler handler = new UpdateHandler();
        final ResourceModel previousModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .build();

        Long updatedDpus = TARGET_DPUS + 12L;
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(updatedDpus)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(previousModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation originalReservation = CapacityReservation.builder()
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

        final CapacityReservation updatingReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(updatedDpus.intValue())
                .status(CapacityReservationStatus.UPDATE_PENDING)
                .creationTime(creationTime)
                .allocatedDpus(TARGET_DPUS.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        final CapacityReservation updatedReservation = CapacityReservation.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(updatedDpus.intValue())
                .status(CapacityReservationStatus.ACTIVE)
                .creationTime(creationTime)
                .allocatedDpus(updatedDpus.intValue())
                .lastSuccessfulAllocationTime(allocationCompletedTime)
                .lastAllocation(CapacityAllocation.builder()
                        .status(CapacityAllocationStatus.SUCCEEDED)
                        .requestTime(allocationRequestTime)
                        .requestCompletionTime(allocationCompletedTime)
                        .build())
                .build();

        // Mock update handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                // Mock existence check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(originalReservation)
                        .build())
                // Mock first stabilization check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(updatingReservation)
                        .build())
                // Mock final stabilization check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(updatedReservation)
                        .build());
        when(sdkClient.updateCapacityReservation(any(UpdateCapacityReservationRequest.class)))
                .thenReturn(UpdateCapacityReservationResponse.builder().build());

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

        // Verify update call was made with new DPU count
        ArgumentCaptor<UpdateCapacityReservationRequest> updateCaptor = ArgumentCaptor.forClass(UpdateCapacityReservationRequest.class);
        verify(sdkClient, times(1)).updateCapacityReservation(updateCaptor.capture());
        UpdateCapacityReservationRequest updateCapacityReservationRequest = updateCaptor.getValue();
        assertThat(updateCapacityReservationRequest.targetDpus()).isEqualTo(updatedDpus.intValue());

        // Verify number of get calls for stabilization
        verify(sdkClient, times(3)).getCapacityReservation(any(GetCapacityReservationRequest.class));

        // Verify no interactions with assignment config or tags
        verify(sdkClient, times(0)).putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class));
        verify(sdkClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(sdkClient, times(0)).tagResource(any(TagResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testUpdateCapacityAssignmentConfig() {
        final UpdateHandler handler = new UpdateHandler();
        final ResourceModel previousModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .capacityAssignmentConfiguration(CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(Arrays.asList(CapacityAssignment.builder()
                                .workgroupNames(Arrays.asList(WORKGROUP_NAME_1))
                                .build()))
                        .build())
                .build();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .capacityAssignmentConfiguration(CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(Arrays.asList(CapacityAssignment.builder()
                                .workgroupNames(Arrays.asList(WORKGROUP_NAME_1, WORKGROUP_NAME_2))
                                .build()))
                        .build())
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(previousModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation originalReservation = CapacityReservation.builder()
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

        // Mock update handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                // Mock existence check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(originalReservation)
                        .build());
        when(sdkClient.putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class)))
                .thenReturn(PutCapacityAssignmentConfigurationResponse.builder().build());

        // Mock read handler SDK calls
        GetCapacityAssignmentConfigurationResponse getCapacityAssignmentConfigurationResponse = GetCapacityAssignmentConfigurationResponse.builder()
                .capacityAssignmentConfiguration(software.amazon.awssdk.services.athena.model.CapacityAssignmentConfiguration.builder()
                        .capacityAssignments(software.amazon.awssdk.services.athena.model.CapacityAssignment.builder()
                                .workGroupNames(WORKGROUP_NAME_1, WORKGROUP_NAME_2)
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

        // Verify update call was made with new assignments
        ArgumentCaptor<PutCapacityAssignmentConfigurationRequest> putConfigCaptor = ArgumentCaptor.forClass(PutCapacityAssignmentConfigurationRequest.class);
        verify(sdkClient, times(1)).putCapacityAssignmentConfiguration(putConfigCaptor.capture());
        PutCapacityAssignmentConfigurationRequest putConfigRequest = putConfigCaptor.getValue();
        assertThat(putConfigRequest.capacityAssignments())
                .isEqualTo(getCapacityAssignmentConfigurationResponse.capacityAssignmentConfiguration().capacityAssignments());

        // Verify no interactions with updating capacity reservation or tags
        verify(sdkClient, times(0)).updateCapacityReservation(any(UpdateCapacityReservationRequest.class));
        verify(sdkClient, times(0)).untagResource(any(UntagResourceRequest.class));
        verify(sdkClient, times(0)).tagResource(any(TagResourceRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testUpdateCapacityAssignmentTags() {
        final UpdateHandler handler = new UpdateHandler();

        Map<String, String> previousUserTags = new HashMap<>();
        previousUserTags.put("userTag1", "userValue1");
        previousUserTags.put("userTag2", "userValue2");

        Map<String, String> previousStackTags = new HashMap<>();
        previousStackTags.put("stackTag1", "stackValue1");
        previousStackTags.put("stackTag2", "stackValue2");

        Map<String, String> previousSystemTags = new HashMap<>();
        previousSystemTags.put("aws:cloudformation:test1", "systemValue1");
        previousSystemTags.put("aws:cloudformation:test2", "systemValue2");

        final ResourceModel previousModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .tags(previousUserTags.keySet().stream()
                        .map(key -> Tag.builder()
                                .key(key)
                                .value(previousUserTags.get(key))
                                .build())
                        .collect(Collectors.toSet()))
                .build();

        Map<String, String> desiredUserTags = new HashMap<>();
        desiredUserTags.put("userTag1", "userValue1");
        desiredUserTags.put("userTag3", "userValue3");

        Map<String, String> desiredStackTags = new HashMap<>();
        desiredStackTags.put("stackTag1", "stackValue1");
        desiredStackTags.put("stackTag3", "stackValue3");

        Map<String, String> desiredSystemTags = new HashMap<>();
        desiredSystemTags.put("aws:cloudformation:test1", "systemValue1");
        desiredSystemTags.put("aws:cloudformation:test3", "systemValue3");

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .targetDpus(TARGET_DPUS)
                .tags(desiredUserTags.keySet().stream()
                        .map(key -> Tag.builder()
                                .key(key)
                                .value(desiredUserTags.get(key))
                                .build())
                        .collect(Collectors.toSet()))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .previousResourceState(previousModel)
                .desiredResourceTags(desiredStackTags)
                .systemTags(desiredSystemTags)
                .previousSystemTags(previousSystemTags)
                .previousResourceTags(previousStackTags)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation originalReservation = CapacityReservation.builder()
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

        // Mock update handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                // Mock existence check
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(originalReservation)
                        .build());
        when(sdkClient.tagResource(any(TagResourceRequest.class)))
                .thenReturn(TagResourceResponse.builder()
                        .build());
        when(sdkClient.untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder()
                        .build());

        // Mock read handler SDK calls
        Map<String, String> responseTags = new HashMap<>();
        responseTags.putAll(desiredUserTags);
        responseTags.putAll(desiredStackTags);
        responseTags.putAll(desiredSystemTags);
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(responseTags.keySet().stream()
                                .map(key -> software.amazon.awssdk.services.athena.model.Tag.builder()
                                        .key(key)
                                        .value(responseTags.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        // Verify new tags were added
        ArgumentCaptor<TagResourceRequest> tagCaptor = ArgumentCaptor.forClass(TagResourceRequest.class);
        verify(sdkClient, times(1)).tagResource(tagCaptor.capture());
        TagResourceRequest tagRequest = tagCaptor.getValue();
        assertThat(tagRequest.tags().stream().anyMatch(tag-> tag.key().equals("userTag3"))).isEqualTo(true);
        assertThat(tagRequest.tags().stream().anyMatch(tag-> tag.key().equals("stackTag3"))).isEqualTo(true);
        assertThat(tagRequest.tags().stream().anyMatch(tag-> tag.key().equals("aws:cloudformation:test3"))).isEqualTo(true);

        // Verify old tags were removed
        ArgumentCaptor<UntagResourceRequest> untagCaptor = ArgumentCaptor.forClass(UntagResourceRequest.class);
        verify(sdkClient, times(1)).untagResource(untagCaptor.capture());
        UntagResourceRequest untagResourceRequest = untagCaptor.getValue();
        assertThat(untagResourceRequest.tagKeys().stream().anyMatch(tag-> tag.equals("userTag2"))).isEqualTo(true);
        assertThat(untagResourceRequest.tagKeys().stream().anyMatch(tag-> tag.equals("stackTag2"))).isEqualTo(true);
        assertThat(untagResourceRequest.tagKeys().stream().anyMatch(tag-> tag.equals("aws:cloudformation:test2"))).isEqualTo(true);


        // Verify no interactions with updating capacity reservation or assignments
        verify(sdkClient, times(0)).updateCapacityReservation(any(UpdateCapacityReservationRequest.class));
        verify(sdkClient, times(0)).putCapacityAssignmentConfiguration(any(PutCapacityAssignmentConfigurationRequest.class));

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testUpdateCapacityReservationNotFound() {
        final UpdateHandler handler = new UpdateHandler();
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

        // Mock read SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Reservation not found")
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
    }

    @Test
    public void testUpdateReservationGenericServiceFailure() {
        final UpdateHandler handler = new UpdateHandler();
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

        // Mock generic Athena error
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenThrow(AthenaException.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }
}
