package io.cdap.plugin.google.drive.source;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.cdap.plugin.google.drive.common.FileFromFolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

public class BeamAdapter {
    public static void main(String[] args) {
        Gson gson = new Gson();
        Configuration myHadoopConfiguration = new Configuration(false);

        String referenceName = "GoogleDriveSourceTest";
        String directoryIdentifier = "/test";
        String fileMetadataProperties = "name,mimeType";
        String modificationDateRange = "lifetime";
        String fileTypesToPull = "documents";
        String authType = "serviceAccount";
        String maxPartitionSize = "0";
        String bodyFormat = "string";
        String docsExportingFormat = "text/plain";
//        String sheetsExportingFormat = "text/csv";
//        String drawingsExportingFormat = "image/svg+xml";
//        String presentationsExportingFormat = "text/plain";
        String serviceAccountType = "filePath";
//        String serviceAccountType = "JSON";
//        String serviceAccountType = "OAuth2";
//        String filter = "mimeType = 'text/csv'";
        String accountFilePath = "/Users/ekaterina.tatanova/Documents/GCP/valid-climber-331709-499365532f72.json";
        String filePath = "/Users/ekaterina.tatanova/Documents/GCP/valid-climber-331709-499365532f72.json";
//        String serviceAccountJSON = "499365532f72fb2a97874e4e38641a3b243136cd";
//        String clientId = "118002851629759763242";
//        String clientSecret = "";

       // String stringToFormat = "{\"referenceName\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"accountFilePath\":\"%s\",\"filePath\":\"%s\"%s}";
//        String stringToFormat = "{\"referenceName\":\"%s\",\"fileMetadataProperties\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"accountFilePath\":\"%s\",\"filePath\":\"%s\"%s}";
        String stringToFormat = "{\"referenceName\":\"%s\",\"directoryIdentifier\":\"%s\",\"fileMetadataProperties\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"accountFilePath\":\"%s\",\"filePath\":\"%s\"%s}";
//        String stringToFormat = "{\"referenceName\":\"%s\",\"directoryIdentifier\":\"%s\",\"fileMetadataProperties\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"filter\":\"%s\",\"accountFilePath\":\"%s\",\"filePath\":\"%s\"%s}";
//        String stringToFormat = "{\"referenceName\":\"%s\",\"directoryIdentifier\":\"%s\",\"fileMetadataProperties\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"sheetsExportingFormat\":\"%s\",\"drawingsExportingFormat\":\"%s\",\"presentationsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"filter\":\"%s\",\"filePath\":\"%s\"%s}";
//        String stringToFormat = "{\"referenceName\":\"%s\",\"directoryIdentifier\":\"%s\",\"fileMetadataProperties\":\"%s\",\"modificationDateRange\":\"%s\",\"fileTypesToPull\":\"%s\",\"authType\":\"%s\",\"maxPartitionSize\":\"%s\",\"bodyFormat\":\"%s\",\"docsExportingFormat\":\"%s\",\"sheetsExportingFormat\":\"%s\",\"drawingsExportingFormat\":\"%s\",\"presentationsExportingFormat\":\"%s\",\"serviceAccountType\":\"%s\",\"filter\":\"%s\",\"filePath\":\"%s\"%s}";
        String propertiesMap = String.format(
                stringToFormat,
                referenceName,
                directoryIdentifier,
                fileMetadataProperties,
                modificationDateRange,
                fileTypesToPull,
                authType,
                maxPartitionSize,
                bodyFormat,
                docsExportingFormat,
//                sheetsExportingFormat,
//                drawingsExportingFormat,
//                presentationsExportingFormat,
                serviceAccountType,
//                filter,
                accountFilePath,
                filePath,
                ""
        );
        String propertiesOfPropertiesMap = String.format(
                "{\"properties\":%s}",
                propertiesMap
        );
        String configJSONString = String.format(
                stringToFormat,
                referenceName,
                directoryIdentifier,
                fileMetadataProperties,
                modificationDateRange,
                fileTypesToPull,
                authType,
                maxPartitionSize,
                bodyFormat,
                docsExportingFormat,
//                sheetsExportingFormat,
//                drawingsExportingFormat,
//                presentationsExportingFormat,
                serviceAccountType,
//                filter,
                accountFilePath,
                filePath,
                String.format(",\"properties\":%s", propertiesOfPropertiesMap)
        );

        GoogleDriveSourceConfig googleDriveSourceConfig = gson.fromJson(
                configJSONString,
                GoogleDriveSourceConfig.class);

        GoogleDriveInputFormatProvider googleDriveInputFormatProvider =
                new GoogleDriveInputFormatProvider(googleDriveSourceConfig);

        myHadoopConfiguration.setClass("mapreduce.job.inputformat.class",
                GoogleDriveInputFormat.class, InputFormat.class);
        myHadoopConfiguration.setClass("key.class", Text.class, Object.class);
        myHadoopConfiguration.setClass("value.class", FileFromFolder.class, Object.class);
        myHadoopConfiguration.set(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON,
                googleDriveInputFormatProvider.getInputFormatConfiguration()
                        .get(GoogleDriveInputFormatProvider.PROPERTY_CONFIG_JSON));

        Pipeline p = Pipeline.create();

        PCollection<KV<Text, FileFromFolder>> pcol = p.apply("read",
                HadoopFormatIO.<Text, FileFromFolder>read()
                        .withConfiguration(myHadoopConfiguration));
//        ).setCoder(KvCoder.of(NullableCoder.of(WritableCoder.of(Text.class)), SerializableCoder.of(FileFromFolder.class)));

//        PCollection<String> strings = pcol.apply(MapElements
//                        .into(TypeDescriptors.strings())
//                        .via(
//                                ((SerializableFunction<KV<Text, FileFromFolder>, String>) input -> {
//                                    Gson gson1 = new Gson();
//                                    return gson1.toJson(input.getValue());
//                                })
//                        )
//                )
//                .setCoder(StringUtf8Coder.of());
//
//        strings.apply(TextIO.write().to("./txt.txt"));

        p.run();
    }
}
