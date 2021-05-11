package software.amazon.athena.preparedstatement;

import java.time.Duration;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.DeletePreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.DeletePreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.PreparedStatement;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
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
    AthenaClient athenaClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        athenaClient = mock(AthenaClient.class);
        proxyClient = MOCK_PROXY(proxy, athenaClient);
    }

    @AfterEach
    public void tear_down() {
        verify(athenaClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(athenaClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DeleteHandler handler = new DeleteHandler();
        when(athenaClient.getPreparedStatement(any(GetPreparedStatementRequest.class)))
            .thenReturn(
                GetPreparedStatementResponse.builder()
                    .preparedStatement(PreparedStatement.builder()
                        .statementName("name")
                        .workGroupName("wg-v2")
                        .description("test")
                        .queryStatement("select ?")
                        .build())
                    .build());
        when(athenaClient.deletePreparedStatement(any(DeletePreparedStatementRequest.class)))
            .thenReturn(DeletePreparedStatementResponse.builder().build());
        final ResourceModel model = ResourceModel.builder()
            .statementName("deleted")
            .workGroup("test")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }
}
