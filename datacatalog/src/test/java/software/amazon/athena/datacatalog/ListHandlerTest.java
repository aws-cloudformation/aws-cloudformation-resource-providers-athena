package software.amazon.athena.datacatalog;

import com.google.common.collect.Lists;
import software.amazon.awssdk.services.athena.model.DataCatalogSummary;
import software.amazon.awssdk.services.athena.model.ListDataCatalogsRequest;
import software.amazon.awssdk.services.athena.model.ListDataCatalogsResponse;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends BaseHandlerTest {

    @Override
    protected BaseHandlerAthena getHandlerInstance() {
        return new ListHandler();
    }

    @Test
    public void testSuccessState_emptyList() {
        final ResourceModel model = ResourceModel.builder().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        when(proxyClient.client().listDataCatalogs(any(ListDataCatalogsRequest.class))).thenReturn(
            ListDataCatalogsResponse.builder()
                .dataCatalogsSummary(Lists.newArrayList())
                .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertSuccessState(response);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isEmpty();
    }

    @Test
    public void testSuccessState() {
        final ResourceModel model = ResourceModel.builder().build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        DataCatalogSummary catalog1 = DataCatalogSummary.builder().catalogName("name1").type("HIVE").build();
        DataCatalogSummary catalog2 = DataCatalogSummary.builder().catalogName("name2").type("HIVE").build();

        when(proxyClient.client().listDataCatalogs(any(ListDataCatalogsRequest.class))).thenReturn(
            ListDataCatalogsResponse.builder()
                .nextToken("nextToken")
                .dataCatalogsSummary(Lists.newArrayList(catalog1, catalog2))
                .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = testHandleRequest(request);

        assertSuccessState(response);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels().get(0).getName()).isEqualTo(catalog1.catalogName());
        assertThat(response.getResourceModels().get(0).getType()).isEqualTo(catalog2.typeAsString());
        assertThat(response.getResourceModels().get(1).getName()).isEqualTo(catalog2.catalogName());
        assertThat(response.getResourceModels().get(1).getType()).isEqualTo(catalog2.typeAsString());
    }
}
