package software.amazon.athena.preparedstatement;

import java.time.Duration;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.CreatePreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.CreatePreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.PreparedStatement;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
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
        final CreateHandler handler = new CreateHandler();
        Answer<GetPreparedStatementResponse> readAnswer = new Answer<GetPreparedStatementResponse>() {
            private int count = 0;

            @Override
            public GetPreparedStatementResponse answer(InvocationOnMock invocationOnMock)
                throws Throwable {
                if (count++ == 0) {
                    throw ResourceNotFoundException.builder()
                        .message("No Prepared Statement with statement name test exists in WorkGroup test")
                        .build();
                }
                return GetPreparedStatementResponse.builder()
                    .preparedStatement(PreparedStatement.builder()
                        .statementName("name")
                        .workGroupName("wg-v2")
                        .description("test")
                        .queryStatement("select ?")
                        .build())
                    .build();
            }
        };
        when(athenaClient.getPreparedStatement(any(GetPreparedStatementRequest.class)))
            .thenAnswer(readAnswer);
        when(athenaClient.createPreparedStatement(any(CreatePreparedStatementRequest.class)))
            .thenReturn(CreatePreparedStatementResponse.builder().build());

        final ResourceModel model = ResourceModel.builder()
            .statementName("name")
            .workGroup("wg-v2")
            .description("test")
            .queryStatement("select ?")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_AlreadyExists() {
        final CreateHandler handler = new CreateHandler();
        when(athenaClient.getPreparedStatement(any(GetPreparedStatementRequest.class)))
            .thenReturn(GetPreparedStatementResponse.builder()
                .preparedStatement(PreparedStatement.builder()
                    .statementName("name")
                    .workGroupName("wg-v2")
                    .description("test")
                    .queryStatement("select ?")
                    .build())
                .build());

        final ResourceModel model = ResourceModel.builder()
            .statementName("name")
            .workGroup("wg-v2")
            .description("test")
            .queryStatement("select ?")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(CfnAlreadyExistsException.class, () ->
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger));

    }
}
