/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.google.drive.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.google.drive.common.GoogleDriveBaseConfig;
import io.cdap.plugin.google.drive.common.GoogleDriveClient;
import io.cdap.plugin.google.drive.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.google.drive.source.utils.BodyFormat;
import io.cdap.plugin.google.drive.source.utils.ExportedType;
import io.cdap.plugin.google.drive.source.utils.ModifiedDateRangeType;
import io.cdap.plugin.google.drive.source.utils.ModifiedDateRangeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configurations for Google Drive Batch Source plugin.
 */
public class GoogleDriveSourceConfig extends GoogleDriveBaseConfig {
  public static final String FILTER = "filter";
  public static final String MODIFICATION_DATE_RANGE = "modificationDateRange";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String FILE_METADATA_PROPERTIES = "fileMetadataProperties";
  public static final String FILE_TYPES_TO_PULL = "fileTypesToPull";
  public static final String MAX_PARTITION_SIZE = "maxPartitionSize";
  public static final String BODY_FORMAT = "bodyFormat";
  public static final String DOCS_EXPORTING_FORMAT = "docsExportingFormat";
  public static final String SHEETS_EXPORTING_FORMAT = "sheetsExportingFormat";
  public static final String DRAWINGS_EXPORTING_FORMAT = "drawingsExportingFormat";
  public static final String PRESENTATIONS_EXPORTING_FORMAT = "presentationsExportingFormat";

  public static final String MODIFICATION_DATE_RANGE_LABEL = "Modification date range";
  public static final String START_DATE_LABEL = "Start date";
  public static final String END_DATE_LABEL = "End date";
  public static final String FILE_TYPES_TO_PULL_LABEL = "File types to pull";
  public static final String BODY_FORMAT_LABEL = "Body output format";

  private static final String IS_VALID_FAILURE_MESSAGE_PATTERN = "'%s' property has invalid value %s";

  @Nullable
  @Name(FILTER)
  @Description("Filter that can be applied to the files in the selected directory. \n" +
    "Filters follow the [Google Drive filters syntax](https://developers.google.com/drive/api/v3/ref-search-terms).")
  @Macro
  protected String filter;

  @Name(MODIFICATION_DATE_RANGE)
  @Description("Filter that narrows set of files by modified date range. \n" +
    "User can select either among predefined or custom entered ranges. \n" +
    "For _Custom_ selection the dates range can be specified via **Start date** and **End date**.")
  @Macro
  protected String modificationDateRange;

  @Nullable
  @Name(START_DATE)
  @Description("Start date for custom modification date range. \n" +
    "Is shown only when 'Custom' range is selected for 'Modification date range' field. \n" +
    "RFC3339 (https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  @Macro
  protected String startDate;

  @Nullable
  @Name(END_DATE)
  @Description("End date for custom modification date range. \n" +
    "Is shown only when 'Custom' range is selected for 'Modification date range' field.\n" +
    "RFC3339 (https://tools.ietf.org/html/rfc3339) format, default timezone is UTC, e.g., 2012-06-04T12:00:00-08:00.")
  @Macro
  protected String endDate;

  @Nullable
  @Name(FILE_METADATA_PROPERTIES)
  @Description("Properties that represent metadata of files. \n" +
    "They will be a part of output structured record.")
  @Macro
  protected String fileMetadataProperties;

  @Name(FILE_TYPES_TO_PULL)
  @Description("Types of files which should be pulled from a specified directory. \n" +
    "The following values are supported: binary (all non-Google Drive formats), Google Documents, " +
    "Google Spreadsheets, Google Drawings, Google Presentations and Google Apps Scripts. \n" +
    "For Google Drive formats user should specify exporting format in **Exporting** section.")
  @Macro
  protected String fileTypesToPull;

  @Name(MAX_PARTITION_SIZE)
  @Description("Maximum body size for each structured record specified in bytes. \n" +
    "Default 0 value means unlimited. Is not applicable for files in Google formats.")
  @Macro
  protected String maxPartitionSize;

  @Name(BODY_FORMAT)
  @Description("Output format for body of file. \"Bytes\" and \"String\" values are available.")
  @Macro
  protected String bodyFormat;

  @Name(DOCS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Documents when converted to structured records.")
  @Macro
  protected String docsExportingFormat;

  @Name(SHEETS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Spreadsheets when converted to structured records.")
  @Macro
  protected String sheetsExportingFormat;

  @Name(DRAWINGS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Drawings when converted to structured records.")
  @Macro
  protected String drawingsExportingFormat;

  @Name(PRESENTATIONS_EXPORTING_FORMAT)
  @Description("MIME type which is used for Google Presentations when converted to structured records.")
  @Macro
  protected String presentationsExportingFormat;
  private transient Schema schema = null;

  public Schema getSchema() {
    if (schema == null) {
      schema = SchemaBuilder.buildSchema(getFileMetadataProperties(), getBodyFormat());
    }
    return schema;
  }

  public void validate(FailureCollector collector) {
    super.validate(collector);

    validateFileTypesToPull(collector);

    validateBodyFormat(collector);

    if (validateModificationDateRange(collector)
      && getModificationDateRangeType().equals(ModifiedDateRangeType.CUSTOM)) {
      if (checkPropertyIsSet(collector, startDate, START_DATE, START_DATE_LABEL)) {
        checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(startDate), startDate, START_DATE,
                             START_DATE_LABEL);
      }
      if (checkPropertyIsSet(collector, endDate, END_DATE, END_DATE_LABEL)) {
        checkPropertyIsValid(collector, ModifiedDateRangeUtils.isValidDateString(endDate), startDate, END_DATE,
                             END_DATE_LABEL);
      }
    }

    validateFileProperties(collector);
  }

  @Override
  protected GoogleDriveClient getDriveClient() throws IOException {
    return new GoogleDriveSourceClient(this);
  }

  private void validateFileTypesToPull(FailureCollector collector) {
    if (!containsMacro(FILE_TYPES_TO_PULL)) {
      if (!Strings.isNullOrEmpty(fileTypesToPull)) {
        List<String> exportedTypeStrings = Arrays.asList(fileTypesToPull.split(","));
        exportedTypeStrings.forEach(exportedTypeString -> {
          try {
            ExportedType.fromValue(exportedTypeString);
          } catch (InvalidPropertyTypeException e) {
            collector.addFailure(e.getMessage(), null).withConfigProperty(FILE_TYPES_TO_PULL);
          }
        });
      }
    }
  }

  private void validateBodyFormat(FailureCollector collector) {
    if (!containsMacro(BODY_FORMAT)) {
      try {
        getBodyFormat();
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(BODY_FORMAT);
      }
    }
  }

  private boolean validateModificationDateRange(FailureCollector collector) {
    if (!containsMacro(MODIFICATION_DATE_RANGE)) {
      try {
        getModificationDateRangeType();
        return true;
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(MODIFICATION_DATE_RANGE);
      }
    }
    return false;
  }

  private void validateFileProperties(FailureCollector collector) {
    if (!containsMacro(FILE_METADATA_PROPERTIES) && !Strings.isNullOrEmpty(fileMetadataProperties)) {
      try {
        SchemaBuilder.buildSchema(getFileMetadataProperties(), getBodyFormat());
      } catch (InvalidPropertyTypeException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(FILE_METADATA_PROPERTIES);
      }
    }
  }

  protected void checkPropertyIsValid(FailureCollector collector, boolean isPropertyValid, String propertyName,
                                      String propertyValue, String propertyLabel) {
    if (isPropertyValid) {
      return;
    }
    collector.addFailure(String.format(IS_VALID_FAILURE_MESSAGE_PATTERN, propertyLabel, propertyValue), null)
      .withConfigProperty(propertyName);
  }

  public ModifiedDateRangeType getModificationDateRangeType() {
    return ModifiedDateRangeType.fromValue(modificationDateRange);
  }

  @Nullable
  public String getFilter() {
    return filter;
  }

  @Nullable
  public String getModificationDateRange() {
    return modificationDateRange;
  }

  List<String> getFileMetadataProperties() {
    if (Strings.isNullOrEmpty(fileMetadataProperties)) {
      return Collections.emptyList();
    }
    return Arrays.asList(fileMetadataProperties.split(","));
  }

  public List<ExportedType> getFileTypesToPull() {
    if (Strings.isNullOrEmpty(fileTypesToPull)) {
      return Collections.emptyList();
    }
    return Arrays.stream(fileTypesToPull.split(","))
      .map(type -> ExportedType.fromValue(type)).collect(Collectors.toList());
  }

  public BodyFormat getBodyFormat() {
    return BodyFormat.fromValue(bodyFormat);
  }

  public Long getMaxPartitionSize() {
    return Long.parseLong(maxPartitionSize);
  }

  public String getDocsExportingFormat() {
    return docsExportingFormat;
  }

  public String getSheetsExportingFormat() {
    return sheetsExportingFormat;
  }

  public String getDrawingsExportingFormat() {
    return drawingsExportingFormat;
  }

  public String getPresentationsExportingFormat() {
    return presentationsExportingFormat;
  }

  @Nullable
  public String getStartDate() {
    return startDate;
  }

  @Nullable
  public String getEndDate() {
    return endDate;
  }
}