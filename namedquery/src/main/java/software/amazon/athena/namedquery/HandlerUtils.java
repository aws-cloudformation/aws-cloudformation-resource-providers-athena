package software.amazon.athena.namedquery;

import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.InvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.exceptions.CfnNotFoundException;

import static software.amazon.athena.namedquery.ResourceModel.TYPE_NAME;

final class HandlerUtils {

  private HandlerUtils() {
    // Intentionally left blank
  }

  private static final String NAMEDQUERY_NOT_FOUND = "does not exist";

  static RuntimeException translateAthenaException(AthenaException e, String resourceIdentifier) {
    if (e instanceof InvalidRequestException) {
      if (e.getMessage() != null && e.getMessage().contains(NAMEDQUERY_NOT_FOUND)) {
        return new CfnNotFoundException(TYPE_NAME, resourceIdentifier, e);
      } else {
        return new CfnInvalidRequestException(e.getMessage(), e);
      }
    } else if (e instanceof InternalServerException) {
      return new CfnGeneralServiceException(e.getMessage(), e);
    }

    return e;
  }
}
