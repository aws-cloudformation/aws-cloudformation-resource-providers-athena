package software.amazon.athena.workgroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.EncryptionConfiguration;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfiguration;
import software.amazon.awssdk.services.athena.model.WorkGroupConfigurationUpdates;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {
  @Test
  void testCreateSdkTagFromCfnTag() {
    Tag tag1 = Tag.builder()
      .key("Author").value("Jeff").build();
    Tag tag2 = Tag.builder()
      .key("Position").value("Founder").build();

    List<Tag> cfnTags = Arrays.asList(tag1, tag2);

    List<software.amazon.awssdk.services.athena.model.Tag> sdkTags =
      new Translator().createSdkTagsFromCfnTags(cfnTags);

    assertThat(sdkTags.size()).isEqualTo(cfnTags.size());
    assertThat(sdkTags.get(0).value()).isEqualTo(cfnTags.get(0).getValue());
    assertThat(sdkTags.get(1).value()).isEqualTo(cfnTags.get(1).getValue());
  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfiguration() {
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfiguration.builder()
      .enforceWorkGroupConfiguration(true)
      .bytesScannedCutoffPerQuery(10)
      .publishCloudWatchMetricsEnabled(true)
      .requesterPaysEnabled(false)
      .resultConfiguration(software.amazon.athena.workgroup.ResultConfiguration.builder()
                                                                               .outputLocation("s3://abc/")
                                                                               .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                                                                                                                                                                .encryptionOption("SSE_S3")
                                                                                                                                                                .build())
                                                                               .build())
      .build();

    WorkGroupConfiguration sdkWorkGroupConfiguration =
      new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery().intValue());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption())
      .isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());

  }

  @Test
  void testCreateSdkWorkgroupConfigurationFromCfnConfigurationWithResultConfigurationNullable() {
    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfiguration.builder()
      .enforceWorkGroupConfiguration(true)
      .bytesScannedCutoffPerQuery(10)
      .publishCloudWatchMetricsEnabled(true)
      .requesterPaysEnabled(false)
      .resultConfiguration(null)
      .build();

    WorkGroupConfiguration sdkWorkGroupConfiguration =
      new Translator().createSdkWorkgroupConfigurationFromCfnConfiguration(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery().intValue());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());

  }

  @Test
  void testCreateSdkConfigurationUpdatesFromCfnConfigurationUpdates() {
    software.amazon.athena.workgroup.WorkGroupConfigurationUpdates cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfigurationUpdates.builder()
      .enforceWorkGroupConfiguration(true)
      .bytesScannedCutoffPerQuery(10)
      .publishCloudWatchMetricsEnabled(true)
      .removeBytesScannedCutoffPerQuery(true)
      .requesterPaysEnabled(false)
      .resultConfigurationUpdates(software.amazon.athena.workgroup.ResultConfigurationUpdates.builder()
                                                                                             .outputLocation("s3://test/")
                                                                                             .encryptionConfiguration(software.amazon.athena.workgroup.EncryptionConfiguration.builder()
                                                                                                                                                                              .encryptionOption("CSE_KMS")
                                                                                                                                                                              .kmsKey("eiifcckijivuintgreglhehugckffvndljgrkfbfcgdg")
                                                                                                                                                                              .build())
                                                                                             .removeOutputLocation(true)
                                                                                             .removeEncryptionConfiguration(true)
                                                                                             .build())
      .build();

    WorkGroupConfigurationUpdates sdkWorkGroupConfigurationUpdates =
      new Translator().createSdkConfigurationUpdatesFromCfnConfigurationUpdates(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfigurationUpdates.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getRemoveBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfigurationUpdates.removeBytesScannedCutoffPerQuery());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveOutputLocation())
      .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeOutputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfigurationUpdates().getRemoveEncryptionConfiguration())
      .isEqualTo(sdkWorkGroupConfigurationUpdates.resultConfigurationUpdates().removeEncryptionConfiguration());

  }

  @Test
  void testCreateSdkConfigurationUpdatesFromCfnConfigurationUpdatesWithResultUpdatesNullable() {
    software.amazon.athena.workgroup.WorkGroupConfigurationUpdates cfnWorkGroupConfiguration = software.amazon.athena.workgroup.WorkGroupConfigurationUpdates.builder()
      .enforceWorkGroupConfiguration(true)
      .bytesScannedCutoffPerQuery(10)
      .publishCloudWatchMetricsEnabled(true)
      .removeBytesScannedCutoffPerQuery(true)
      .requesterPaysEnabled(false)
      .resultConfigurationUpdates(null)
      .build();

    WorkGroupConfigurationUpdates sdkWorkGroupConfigurationUpdates =
      new Translator().createSdkConfigurationUpdatesFromCfnConfigurationUpdates(cfnWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfigurationUpdates.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfigurationUpdates.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getRemoveBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfigurationUpdates.removeBytesScannedCutoffPerQuery());

  }


  @Test
  void testCreateCfnWorkgroupConfigurationFromSdkConfiguration() {
    WorkGroupConfiguration sdkWorkGroupConfiguration = WorkGroupConfiguration.builder()
                                                                             .enforceWorkGroupConfiguration(true)
                                                                             .bytesScannedCutoffPerQuery(10L)
                                                                             .publishCloudWatchMetricsEnabled(true)
                                                                             .requesterPaysEnabled(false)
                                                                             .resultConfiguration(ResultConfiguration.builder()
                                                                                                                     .outputLocation("s3://abc/")
                                                                                                                     .encryptionConfiguration(EncryptionConfiguration.builder()
                                                                                                                                                                     .encryptionOption("CSE_KMS")
                                                                                                                                                                     .kmsKey("eiifcckijivuujrbgftugjutulndhjdbdvuihldhriee")
                                                                                                                                                                     .build())
                                                                                                                     .build())
      .build();

    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = new Translator().createCfnWorkgroupConfigurationFromSdkConfiguration(sdkWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery().intValue());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getOutputLocation()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().outputLocation());
    assertThat(cfnWorkGroupConfiguration.getResultConfiguration().getEncryptionConfiguration().getEncryptionOption()).isEqualTo(sdkWorkGroupConfiguration.resultConfiguration().encryptionConfiguration().encryptionOption().toString());
  }


  @Test
  void testCreateCfnWorkgroupConfigurationFromSdkConfigurationResultConfigurationNullable() {
    WorkGroupConfiguration sdkWorkGroupConfiguration = WorkGroupConfiguration.builder()
      .enforceWorkGroupConfiguration(true)
      .bytesScannedCutoffPerQuery(10L)
      .publishCloudWatchMetricsEnabled(true)
      .requesterPaysEnabled(false)
      .build();

    software.amazon.athena.workgroup.WorkGroupConfiguration cfnWorkGroupConfiguration = new Translator().createCfnWorkgroupConfigurationFromSdkConfiguration(sdkWorkGroupConfiguration);

    assertThat(cfnWorkGroupConfiguration.getEnforceWorkGroupConfiguration()).isEqualTo(sdkWorkGroupConfiguration.enforceWorkGroupConfiguration());
    assertThat(cfnWorkGroupConfiguration.getBytesScannedCutoffPerQuery()).isEqualTo(sdkWorkGroupConfiguration.bytesScannedCutoffPerQuery().intValue());
    assertThat(cfnWorkGroupConfiguration.getPublishCloudWatchMetricsEnabled()).isEqualTo(sdkWorkGroupConfiguration.publishCloudWatchMetricsEnabled());
    assertThat(cfnWorkGroupConfiguration.getRequesterPaysEnabled()).isEqualTo(sdkWorkGroupConfiguration.requesterPaysEnabled());
  }
}
