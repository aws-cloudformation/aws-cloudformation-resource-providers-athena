package software.amazon.athena.preparedstatement;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import software.amazon.awssdk.services.athena.model.CreatePreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.DeletePreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementRequest;
import software.amazon.awssdk.services.athena.model.GetPreparedStatementResponse;
import software.amazon.awssdk.services.athena.model.ListPreparedStatementsRequest;
import software.amazon.awssdk.services.athena.model.ListPreparedStatementsResponse;
import software.amazon.awssdk.services.athena.model.PreparedStatement;
import software.amazon.awssdk.services.athena.model.UpdatePreparedStatementRequest;

/**
 * This class is a centralized placeholder for
 *  - api request construction
 *  - object translation to/from aws sdk
 *  - resource model construction for read/list handlers
 */

public class Translator {

  /**
   * Request to create a resource
   * @param model resource model
   * @return awsRequest the aws service request to create a resource
   */
  static CreatePreparedStatementRequest translateToCreateRequest(final ResourceModel model) {
    final CreatePreparedStatementRequest awsRequest = CreatePreparedStatementRequest.builder()
        .statementName(model.getStatementName())
        .workGroup(model.getWorkGroup())
        .description(model.getDescription())
        .queryStatement(model.getQueryStatement())
        .build();
    return awsRequest;
  }

  /**
   * Request to read a resource
   * @param model resource model
   * @return awsRequest the aws service request to describe a resource
   */
  static GetPreparedStatementRequest translateToReadRequest(final ResourceModel model) {
    GetPreparedStatementRequest awsRequest;
    awsRequest = GetPreparedStatementRequest.builder()
        .statementName(model.getStatementName())
        .workGroup(model.getWorkGroup())
        .build();
    return awsRequest;
  }

  /**
   * Translates resource object from sdk into a resource model
   * @param awsResponse the aws service describe resource response
   * @return model resource model
   */
  static ResourceModel translateFromReadResponse(final AwsResponse awsResponse) {
    PreparedStatement readResponse = ((GetPreparedStatementResponse)awsResponse).preparedStatement();
    return ResourceModel.builder()
        .statementName(readResponse.statementName())
        .workGroup(readResponse.workGroupName())
        .description(readResponse.description())
        .queryStatement(readResponse.queryStatement())
        .build();
  }

  /**
   * Request to delete a resource
   * @param model resource model
   * @return awsRequest the aws service request to delete a resource
   */
  static DeletePreparedStatementRequest translateToDeleteRequest(final ResourceModel model) {
    final DeletePreparedStatementRequest awsRequest = DeletePreparedStatementRequest.builder()
        .statementName(model.getStatementName())
        .workGroup(model.getWorkGroup())
        .build();
    return awsRequest;
  }

  /**
   * Request to update properties of a previously created resource
   * @param model resource model
   * @return awsRequest the aws service request to modify a resource
   */
  static UpdatePreparedStatementRequest translateToFirstUpdateRequest(final ResourceModel model) {
    final UpdatePreparedStatementRequest awsRequest = UpdatePreparedStatementRequest.builder()
        .statementName(model.getStatementName())
        .workGroup(model.getWorkGroup())
        .queryStatement(model.getQueryStatement())
        .description(model.getDescription())
        .build();
    return awsRequest;
  }

  /**
   * Request to list resources
   * @param nextToken token passed to the aws service list resources request
   * @return awsRequest the aws service request to list resources within aws account
   */
  static ListPreparedStatementsRequest translateToListRequest(final ResourceModel model, final String nextToken) {
    final ListPreparedStatementsRequest awsRequest = ListPreparedStatementsRequest.builder()
        .nextToken(nextToken)
        .workGroup(model.getWorkGroup())
        .maxResults(50) // max is 50
        .build();
    return awsRequest;
  }

  /**
   * Translates resource objects from sdk into a resource model (primary identifier only)
   * @param response the aws service describe resource response
   * @return list of resource models
   */
  static List<ResourceModel> translateFromListRequest(final ListPreparedStatementsResponse response, String workgroup) {
    return streamOfOrEmpty(response.preparedStatements())
        .map(psSummary -> ResourceModel.builder()
            .statementName(psSummary.statementName())
            .workGroup(workgroup)
            .build())
        .collect(Collectors.toList());
  }

  private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
    return Optional.ofNullable(collection)
        .map(Collection::stream)
        .orElseGet(Stream::empty);
  }
}
