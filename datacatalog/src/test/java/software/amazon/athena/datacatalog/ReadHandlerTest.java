package software.amazon.athena.datacatalog;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import software.amazon.awssdk.services.athena.model.DataCatalog;
import software.amazon.awssdk.services.athena.model.GetDataCatalogRequest;
import software.amazon.awssdk.services.athena.model.GetDataCatalogResponse;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.athena.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends BaseHandlerTest {

    @Override
    protected BaseHandlerAthena getHandlerInstance() {
        return new ReadHandler();
    }

    @Test
    public void testSuccessState() {

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .region("")
            .awsAccountId("")
            .desiredResourceState(buildSimpleTestResourceModel())
            .build();

        final DataCatalog expected = DataCatalog.builder()
            .name("TestCatalog")
            .type("HIVE")
            .description("hello world !!!")
            .parameters(ImmutableMap.of("metadata-function", "testing"))
            .status("CREATE_COMPLETE")
            .build();
        // Mock
        when(proxyClient.client().getDataCatalog(any(GetDataCatalogRequest.class)))
            .thenReturn(GetDataCatalogResponse.builder()
                .dataCatalog(expected)
                .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
        .thenReturn(ListTagsForResourceResponse.builder()
                .tags(Arrays.asList(
                    software.amazon.awssdk.services.athena.model.Tag.builder()
                        .key("testK").value("testV")
                        .build()))
                .nextToken("1stinvocation")
                .build())
        .thenReturn(ListTagsForResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertThat(response.getResourceModel().getName()).isEqualTo(expected.name());
        assertThat(response.getResourceModel().getType()).isEqualTo(expected.type().toString());
        assertThat(response.getResourceModel().getDescription()).isEqualTo(expected.description());
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo("testK");
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo("testV");
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    public void testFederatedSuccessState() {

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .region("")
                .awsAccountId("")
                .desiredResourceState(buildTestResourceModelFederated())
                .build();

        final DataCatalog expected = DataCatalog.builder()
                .name("TestCatalog")
                .type("FEDERATED")
                .description("hello world !!!")
                .parameters(ImmutableMap.of("metadata-function", "testing"))
                .status("CREATE_COMPLETE")
                .error("No error")
                .connectionType("DYNAMODB")
                .build();
        // Mock
        when(proxyClient.client().getDataCatalog(any(GetDataCatalogRequest.class)))
                .thenReturn(GetDataCatalogResponse.builder()
                        .dataCatalog(expected)
                        .build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tags(Arrays.asList(
                                software.amazon.awssdk.services.athena.model.Tag.builder()
                                        .key("testK").value("testV")
                                        .build()))
                        .nextToken("1stinvocation")
                        .build())
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertThat(response.getResourceModel().getName()).isEqualTo(expected.name());
        assertThat(response.getResourceModel().getType()).isEqualTo(expected.type().toString());
        assertThat(response.getResourceModel().getDescription()).isEqualTo(expected.description());
        assertThat(response.getResourceModel().getError()).isEqualTo(expected.error());
        assertThat(response.getResourceModel().getStatus()).isEqualTo(expected.statusAsString());
        assertThat(response.getResourceModel().getConnectionType()).isEqualTo(expected.connectionTypeAsString());
        assertThat(response.getResourceModel().getTags().get(0).getKey()).isEqualTo("testK");
        assertThat(response.getResourceModel().getTags().get(0).getValue()).isEqualTo("testV");
        assertThat(response.getResourceModels()).isNull();
    }

    @Test
    public void testSuccessState_ResourceNotFound() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .region("")
            .awsAccountId("")
            .desiredResourceState(buildSimpleTestResourceModel())
            .build();

        // Mock
        when(proxyClient.client().getDataCatalog(any(GetDataCatalogRequest.class)))
        .thenThrow(InvalidRequestException.builder().message("datacatalog was not found").build());

        assertThrows(CfnNotFoundException.class, () -> testHandleRequest(request));
    }
}
