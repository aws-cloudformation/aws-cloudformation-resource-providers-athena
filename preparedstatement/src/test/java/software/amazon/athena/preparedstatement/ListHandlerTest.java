package software.amazon.athena.preparedstatement;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.athena.model.ListPreparedStatementsResponse;
import software.amazon.awssdk.services.athena.model.PreparedStatementSummary;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;


    @BeforeEach
    public void setup() {
        proxy = mock(AmazonWebServicesClientProxy.class);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final ListHandler handler = new ListHandler();
        List<PreparedStatementSummary> expected = Lists.newArrayList(
            PreparedStatementSummary.builder().statementName("ps1").build(),
            PreparedStatementSummary.builder().statementName("ps2").build());
        when(proxy.injectCredentialsAndInvokeV2(any(), any()))
            .thenReturn(ListPreparedStatementsResponse.builder()
                .nextToken("anexttoken")
                .preparedStatements(expected)
                .build());

        final ResourceModel model = ResourceModel.builder()
            .workGroup("testwg")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, new CallbackContext(), logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();

        Set<String> results = response.getResourceModels().stream()
            .map(ResourceModel::getStatementName)
            .collect(Collectors.toSet());
        assertEquals(results, Sets.newHashSet("ps1", "ps2"));
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

    }
}
