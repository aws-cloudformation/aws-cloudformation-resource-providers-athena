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
import software.amazon.awssdk.services.athena.model.CapacityAssignmentConfiguration;
import software.amazon.awssdk.services.athena.model.CapacityReservation;
import software.amazon.awssdk.services.athena.model.CapacityReservationStatus;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityAssignmentConfigurationResponse;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationRequest;
import software.amazon.awssdk.services.athena.model.GetCapacityReservationResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.athena.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

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
    public void testReadCapacityReservationBasic() {
        final ReadHandler handler = new ReadHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
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
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        // Mock no capacity assignment
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        // Mock no tags
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ResourceModel actualResourceModel = response.getResourceModel();
        String expectedArn = Translator.translateToCapacityReservationArn(AWS_PARTITION, AWS_REGION, ACCOUNT_ID, expectedCapacityReservation.name());
        assertThat(actualResourceModel.getArn()).isEqualTo(expectedArn);
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertThat(actualResourceModel.getCapacityAssignmentConfiguration()).isNull();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadCapacityReservationWithOnlyArn() {
        final ReadHandler handler = new ReadHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .arn(Translator.translateToCapacityReservationArn(AWS_PARTITION,
                        AWS_REGION, ACCOUNT_ID, CAPACITY_RESERVATION_NAME))
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
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        // Mock no capacity assignment
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        // Mock no tags
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ResourceModel actualResourceModel = response.getResourceModel();
        String expectedArn = Translator.translateToCapacityReservationArn(AWS_PARTITION, AWS_REGION, ACCOUNT_ID, expectedCapacityReservation.name());
        assertThat(actualResourceModel.getArn()).isEqualTo(expectedArn);
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertThat(actualResourceModel.getCapacityAssignmentConfiguration()).isNull();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadReservationWithAssignment() {
        final ReadHandler handler = new ReadHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
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
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        final CapacityAssignmentConfiguration expectedCapacityAssignmentConfig = CapacityAssignmentConfiguration.builder()
                        .capacityReservationName(CAPACITY_RESERVATION_NAME)
                        .capacityAssignments(software.amazon.awssdk.services.athena.model.CapacityAssignment.builder()
                                .workGroupNames(WORKGROUP_NAME_1)
                                .build())
                        .build();

        // Mock read handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        GetCapacityAssignmentConfigurationResponse getCapacityAssignmentConfigurationResponse = GetCapacityAssignmentConfigurationResponse.builder()
                .capacityAssignmentConfiguration(expectedCapacityAssignmentConfig)
                .build();
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenReturn(getCapacityAssignmentConfigurationResponse);
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        ResourceModel actualResourceModel = response.getResourceModel();
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertExpectedAssignmentConfiguration(actualResourceModel.getCapacityAssignmentConfiguration(),
                expectedCapacityAssignmentConfig);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadReservationWithTags() {
        final ReadHandler handler = new ReadHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        Map<String, String> responseTags = new HashMap<>();
        responseTags.put("userTag1", "userValue1");
        responseTags.put("userTag2", "userValue2");
        responseTags.put("stackTag1", "stackValue1");
        responseTags.put("stackTag2", "stackValue2");
        responseTags.put("aws:cloudformation:test1", "systemValue1");
        responseTags.put("aws:cloudformation:test2", "systemValue2");

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(responseTags.keySet().stream()
                                .map(key -> Tag.builder()
                                        .key(key)
                                        .value(responseTags.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        ResourceModel actualResourceModel = response.getResourceModel();
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertExpectedModelTags(actualResourceModel.getTags(), responseTags);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadReservationWithPaginatedTags() {
        final ReadHandler handler = new ReadHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        Map<String, String> responseTagsBatch1 = new HashMap<>();
        responseTagsBatch1.put("userTag1", "userValue1");
        responseTagsBatch1.put("userTag2", "userValue2");

        Map<String, String> responseTagsBatch2 = new HashMap<>();
        responseTagsBatch2.put("aws:cloudformation:test1", "systemValue1");
        responseTagsBatch2.put("aws:cloudformation:test2", "systemValue2");

        Map<String, String> responseTagsBatch3 = new HashMap<>();
        responseTagsBatch3.put("stackTag1", "stackValue1");
        responseTagsBatch3.put("stackTag2", "stackValue2");


        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());

        String nextTokenForFirstCall = UUID.randomUUID().toString();
        String nextTokenForSecondCall = UUID.randomUUID().toString();
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(responseTagsBatch1.keySet().stream()
                                .map(key -> Tag.builder()
                                        .key(key)
                                        .value(responseTagsBatch1.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .nextToken(nextTokenForFirstCall)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(responseTagsBatch2.keySet().stream()
                                .map(key -> Tag.builder()
                                        .key(key)
                                        .value(responseTagsBatch2.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .nextToken(nextTokenForSecondCall)
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(responseTagsBatch3.keySet().stream()
                                .map(key -> Tag.builder()
                                        .key(key)
                                        .value(responseTagsBatch3.get(key))
                                        .build())
                                .collect(Collectors.toList()))
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        ArgumentCaptor<ListTagsForResourceRequest> listTagsRequestCaptor = ArgumentCaptor.forClass(ListTagsForResourceRequest.class);
        verify(sdkClient, times(3)).listTagsForResource(listTagsRequestCaptor.capture());
        List<ListTagsForResourceRequest> listTagRequests = listTagsRequestCaptor.getAllValues();
        assertThat(listTagRequests.stream()
                .map(listTagReq -> listTagReq.nextToken())
                .distinct()
                .collect(Collectors.toList())
                .size()).isEqualTo(3);

        ResourceModel actualResourceModel = response.getResourceModel();
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);

        Map<String, String> expectedTags = responseTagsBatch1;
        expectedTags.putAll(responseTagsBatch2);
        expectedTags.putAll(responseTagsBatch3);
        assertExpectedModelTags(actualResourceModel.getTags(), expectedTags);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadCapacityReservationNotFound() {
        final ReadHandler handler = new ReadHandler();
        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
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
    public void testReadReservationWithTagsNotAuthorized() {
        final ReadHandler handler = new ReadHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
                .build();

        Map<String, String> responseTags = new HashMap<>();
        responseTags.put("userTag1", "userValue1");
        responseTags.put("userTag2", "userValue2");
        responseTags.put("stackTag1", "stackValue1");
        responseTags.put("stackTag2", "stackValue2");
        responseTags.put("aws:cloudformation:test1", "systemValue1");
        responseTags.put("aws:cloudformation:test2", "systemValue2");

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(desiredModel)
                .awsAccountId(ACCOUNT_ID)
                .awsPartition(AWS_PARTITION)
                .region(AWS_REGION)
                .build();

        Instant creationTime = Instant.now();
        Instant allocationRequestTime = creationTime.plusMillis(1000);
        Instant allocationCompletedTime = allocationRequestTime.plusMillis(1000);
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(InvalidRequestException.builder()
                        .message("Resource not found")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenThrow(AthenaException.builder()
                        .message("You are not authorized to perform: athena:ListTagsForResource on the resource. After your AWS administrator or you have updated your permissions, please try again.")
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        ResourceModel actualResourceModel = response.getResourceModel();
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertThat(actualResourceModel.getTags()).isNull();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void testReadReservationWithAssignmentNotAuthorized() {
        final ReadHandler handler = new ReadHandler();

        final ResourceModel desiredModel = ResourceModel.builder()
                .name(CAPACITY_RESERVATION_NAME)
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
        final CapacityReservation expectedCapacityReservation = CapacityReservation.builder()
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

        // Mock read handler SDK calls
        when(sdkClient.getCapacityReservation(any(GetCapacityReservationRequest.class)))
                .thenReturn(GetCapacityReservationResponse.builder()
                        .capacityReservation(expectedCapacityReservation)
                        .build());
        when(sdkClient.getCapacityAssignmentConfiguration(any(GetCapacityAssignmentConfigurationRequest.class)))
                .thenThrow(AthenaException.builder()
                        .message("You are not authorized to perform: athena:GetCapacityAssignmentConfiguration on the resource. After your AWS administrator or you have updated your permissions, please try again.")
                        .build());
        when(sdkClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Collections.emptyList())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);
        ResourceModel actualResourceModel = response.getResourceModel();
        assertExpectedModel(actualResourceModel, expectedCapacityReservation);
        assertThat(actualResourceModel.getCapacityAssignmentConfiguration()).isNull();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }



    private void assertExpectedModel(ResourceModel actualModel,
                                     CapacityReservation expectedReservation) {
        assertThat(actualModel.getName()).isEqualTo(expectedReservation.name());
        assertThat(actualModel.getTargetDpus().intValue()).isEqualTo(expectedReservation.targetDpus());
        assertThat(actualModel.getAllocatedDpus().intValue()).isEqualTo(expectedReservation.allocatedDpus());
        assertThat(actualModel.getStatus()).isEqualTo(expectedReservation.statusAsString());
        assertThat(actualModel.getCreationTime()).isEqualTo(Long.toString(expectedReservation.creationTime().getEpochSecond()));
        assertThat(actualModel.getLastSuccessfulAllocationTime()).isEqualTo(Long.toString(expectedReservation.lastSuccessfulAllocationTime().getEpochSecond()));
    }

    private void assertExpectedAssignmentConfiguration(
            software.amazon.athena.capacityreservation.CapacityAssignmentConfiguration modelConfiguration,
            CapacityAssignmentConfiguration expectedConfiguration) {
            assertThat(modelConfiguration.getCapacityAssignments().size())
                    .isEqualTo(expectedConfiguration.capacityAssignments().size());
            modelConfiguration.getCapacityAssignments()
                    .stream()
                    .forEach(capacityAssignment -> {
                        software.amazon.awssdk.services.athena.model.CapacityAssignment assignment =
                                software.amazon.awssdk.services.athena.model.CapacityAssignment.builder()
                                .workGroupNames(capacityAssignment.getWorkgroupNames())
                                .build();
                        assertThat(expectedConfiguration.capacityAssignments().contains(assignment));
                    });
    }
}
