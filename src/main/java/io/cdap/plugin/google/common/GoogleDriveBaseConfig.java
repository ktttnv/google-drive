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

package io.cdap.plugin.google.common;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;

import java.io.File;
import java.io.IOException;

/**
 * Base Google Drive batch config. Contains common configuration properties and methods.
 */
public abstract class GoogleDriveBaseConfig extends PluginConfig {
  public static final String AUTO_DETECT_VALUE = "auto-detect";
  public static final String REFERENCE_NAME = "referenceName";
  public static final String ACCOUNT_FILE_PATH = "accountFilePath";
  public static final String DIRECTORY_IDENTIFIER = "directoryIdentifier";

  @Name(REFERENCE_NAME)
  @Description("Reference Name")
  protected String referenceName;

  // TODO remove these properties after OAuth2 will be provided by cdap
  // start of workaround
  @Name(ACCOUNT_FILE_PATH)
  @Description("Path on the local file system of the user/service account key used for authorization. " +
    "Can be set to 'auto-detect' when running on a Dataproc cluster. " +
    "When running on other clusters, the file must be present on every node in the cluster." +
    "Required minimal json format for user account:" +
    "{" +
    "  \"client_id\": \"<clientId>\"," +
    "  \"client_secret\": \"<clientSecret>\"," +
    "  \"refresh_token\": \"<refreshToken>\"," +
    "  \"type\": \"authorized_user\"" +
    "}" +
    "Service account json can be generated on Google Cloud " +
    "Service Account page (https://console.cloud.google.com/iam-admin/serviceaccounts)")
  @Macro
  protected String accountFilePath;
  // end of workaround

  @Name(DIRECTORY_IDENTIFIER)
  @Description("ID of target directory, the last part of the URL.")
  @Macro
  protected String directoryIdentifier;

  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);

    if (validateAccountFilePath(collector)) {
      try {
        GoogleDriveClient client = getDriveClient();

        // validate auth
        validateCredentials(collector, client);

        // validate directory
        validateDirectoryIdentifier(collector, client);

      } catch (Exception e) {
        collector.addFailure(
          String.format("Exception during authentication/directory properties check: %s", e.getMessage()),
          "Check message and reconfigure the plugin")
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  protected abstract GoogleDriveClient getDriveClient();

  private boolean validateAccountFilePath(FailureCollector collector) {
    if (!containsMacro(ACCOUNT_FILE_PATH)) {
      if (!AUTO_DETECT_VALUE.equals(accountFilePath) && !new File(accountFilePath).exists()) {
        collector.addFailure("Account file is not available",
                             "Provide path to existing account file")
          .withConfigProperty(ACCOUNT_FILE_PATH);
        return false;
      }
      return true;
    }
    return false;
  }

  private void validateCredentials(FailureCollector collector, GoogleDriveClient driveClient) throws IOException {
    if (!containsMacro(ACCOUNT_FILE_PATH)) {
      try {
        driveClient.checkRootFolder();
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getMessage(), "Provide valid credentials")
          .withConfigProperty(ACCOUNT_FILE_PATH)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  private void validateDirectoryIdentifier(FailureCollector collector, GoogleDriveClient driveClient)
    throws IOException {
    if (!containsMacro(DIRECTORY_IDENTIFIER)) {
      try {
        driveClient.isFolderAccessible(directoryIdentifier);
      } catch (GoogleJsonResponseException e) {
        collector.addFailure(e.getMessage(), "Provide an existing folder identifier")
          .withConfigProperty(DIRECTORY_IDENTIFIER)
          .withStacktrace(e.getStackTrace());
      }
    }
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getDirectoryIdentifier() {
    return directoryIdentifier;
  }

  public String getAccountFilePath() {
    return accountFilePath;
  }
}
