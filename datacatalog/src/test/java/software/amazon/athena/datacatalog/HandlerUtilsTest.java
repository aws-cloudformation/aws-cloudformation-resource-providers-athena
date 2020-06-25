package software.amazon.athena.datacatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.athena.datacatalog.HandlerUtils.handleExceptions;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ResourceNotFoundException;
import software.amazon.awssdk.services.athena.model.TooManyRequestsException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;

public class HandlerUtilsTest {

  @Test
  public void testHandleExceptions_default() {
    AthenaException exception = TooManyRequestsException.builder().build();
    assertThat(handleExceptions(exception, "name")).isEqualTo(exception);
  }

  @Test
  public void testHandleExceptions_ResourceNotFoundException() {
    AthenaException exception = ResourceNotFoundException.builder().build();
    assertThat(handleExceptions(exception, "name")).isInstanceOf(CfnNotFoundException.class);
  }
}
