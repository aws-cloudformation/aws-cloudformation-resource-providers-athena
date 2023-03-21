package software.amazon.athena.datacatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaRequest;
import software.amazon.awssdk.services.athena.model.AthenaResponse;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.CreateDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.TagResourceRequest;
import software.amazon.awssdk.services.athena.model.TagResourceResponse;
import software.amazon.awssdk.services.athena.model.UntagResourceRequest;
import software.amazon.awssdk.services.athena.model.UntagResourceResponse;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.UpdateDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupRequest;
import software.amazon.awssdk.services.athena.model.UpdateWorkGroupResponse;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends BaseHandlerTest {

    @Mock
    ReadHandler readHandler;

    @Override
    protected BaseHandlerAthena getHandlerInstance() {
        UpdateHandler u = new UpdateHandler();
        u.setReadHandler(readHandler);
        return u;
    }

    @BeforeEach
    public void setUpReader() {
        readHandler = mock(ReadHandler.class);
    }

    @Test
    public void testSuccessState_withNullTags() {
        final ResourceModel model = buildTestResourceModelWithNullTags();
        final ResourceModel model2 = buildTestResourceModelWithNullTags();
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(model, model2);

        mockInvocation(false, false);
        mockSuccessfulReadhandler(model);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertDesiredState(response, request);
    }

    @Test
    public void testSuccessState_WithAddingTags() {
        final ResourceModel oldModel = buildTestResourceModelWithNullTags(); // no tags
        final ResourceModel newModel = buildTestResourceModel(); // with tags
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(oldModel, newModel);

        mockInvocation(true, false);
        mockSuccessfulReadhandler(oldModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        assertDesiredState(response, request);

        // Verify that tags are added
        TagResourceRequest tagRequest =
            captureRequests(verifiedClient()::tagResource, TagResourceRequest.class).get(0);
        assertThat(tagRequest.tags().containsAll(newModel.getTags()));
    }

    @Test
    public void testSuccessState_WithAddingSystemTags() {
        final ResourceModel resourceModel = buildTestResourceModelWithNullTags();
        Map<String, String> systemTags = new HashMap<>();
        systemTags.put("aws:tag:somekey", "somevalue");
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                                                                      .desiredResourceState(resourceModel)
                                                                      .systemTags(systemTags)
                                                                      .region("region")
                                                                      .awsAccountId("account")
                                                                      .build();

        mockInvocation(true, false);
        mockSuccessfulReadhandler(resourceModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        assertDesiredState(response, request);

        // Verify that tags are added
        TagResourceRequest tagRequest =
                captureRequests(verifiedClient()::tagResource, TagResourceRequest.class).get(0);
        assertThat(tagRequest.tags().containsAll(systemTags.entrySet()));
    }

    @Test
    public void testSuccessState_WithRemovingTags() {
        final ResourceModel oldModel = buildTestResourceModel(); // with tags
        final ResourceModel newModel = buildTestResourceModelWithNullTags(); // no tags
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(oldModel, newModel);

        mockInvocation(false, true);
        mockSuccessfulReadhandler(oldModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        assertDesiredState(response, request);

        // Verify that tags are removed
        UntagResourceRequest untagResourceRequest =
            captureRequests(verifiedClient()::untagResource, UntagResourceRequest.class).get(0);
        assertThat(untagResourceRequest.tagKeys().containsAll(
            oldModel.getTags().stream().map(Tag::getKey).collect(Collectors.toList())));
    }

    @Test
    public void testSuccessState_WithUpdatingTagValue() {
        final ResourceModel oldModel = buildTestResourceModel();
        final ResourceModel newModel = buildTestResourceModelWithUpdatedTagValue();
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(oldModel, newModel);

        mockInvocation(true, true);
        mockSuccessfulReadhandler(oldModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        assertDesiredState(response, request);

        // Verify that tags are removed and added
        TagResourceRequest tagRequest =
            captureRequests(verifiedClient()::tagResource, TagResourceRequest.class).get(0);
        UntagResourceRequest untagResourceRequest =
            captureRequests(verifiedClient()::untagResource, UntagResourceRequest.class).get(0);
        assertThat(tagRequest.tags().containsAll(newModel.getTags()));
        assertThat(untagResourceRequest.tagKeys().containsAll(
            oldModel.getTags().stream().map(Tag::getKey).collect(Collectors.toList())));
    }

    @Test
    public void testSuccessState_UpdateParameters() {
        final ResourceModel oldModel = buildTestResourceModel();
        final ResourceModel newModel = buildTestResourceModelWithUpdatedParameters();
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(oldModel, newModel);

        mockInvocation(false, false);
        mockSuccessfulReadhandler(oldModel);
        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);
        assertDesiredState(response, request);

        List<UpdateDataCatalogRequest> requests = captureRequests(verifiedClient()::updateDataCatalog, UpdateDataCatalogRequest.class);
        UpdateDataCatalogRequest updateRequest = requests.get(0);
        assertThat(updateRequest.parameters()).isEqualTo(newModel.getParameters());
    }

    @Test
    public void testFailedState_ExceptionHandling() {
        final ResourceModel oldModel = buildTestResourceModel();
        final ResourceModel newModel = buildTestResourceModelWithUpdatedParameters();
        final ResourceHandlerRequest<ResourceModel> request = getUpdateResourceHandlerRequest(oldModel, newModel);

        mockSuccessfulReadhandler(oldModel);
        when(proxyClient.client().updateDataCatalog(any(UpdateDataCatalogRequest.class)))
            .thenThrow(InternalServerException.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
    }

    private ResourceHandlerRequest<ResourceModel> getUpdateResourceHandlerRequest(ResourceModel oldModel,
            ResourceModel newModel) {
        return ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(newModel)
            .previousResourceState(oldModel)
            .region("region")
            .awsAccountId("account")
            .build();
    }

    private void mockInvocation(boolean tag, boolean untag) {
        when(proxyClient.client().updateDataCatalog(any(UpdateDataCatalogRequest.class)))
            .thenReturn(UpdateDataCatalogResponse.builder().build());
        if (untag) {
            when(proxyClient.client().untagResource(any(UntagResourceRequest.class)))
                .thenReturn(UntagResourceResponse.builder().build());
        }
        if (tag) {
            when(proxyClient.client().tagResource(any(TagResourceRequest.class)))
                .thenReturn(TagResourceResponse.builder().build());
        }
    }

    private void mockSuccessfulReadhandler(ResourceModel model) {
        when(readHandler.handleRequest(any(), any(), any(), any(), any()))
            .thenReturn(ProgressEvent.defaultSuccessHandler(model));
    }

    private void assertDesiredState(ProgressEvent<ResourceModel, CallbackContext> response,
        ResourceHandlerRequest<ResourceModel> request) {
        assertSuccessState(response);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
    }

    private <T extends AthenaRequest, F extends AthenaResponse> List<T> captureRequests(
            Function<T, F> function, Class<T> clazz) {
        ArgumentCaptor<T> requestCaptor = ArgumentCaptor.forClass(clazz);
        function.apply(requestCaptor.capture());
        return requestCaptor.getAllValues();
    }

    private AthenaClient verifiedClient() {
        return verify(proxyClient.client());
    }
}
