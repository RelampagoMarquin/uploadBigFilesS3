package com.example;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.Tag;

public class DownloadLargeFileBracketedByChecksum {
    public static void downloadLargeFileBracketedByChecksum(S3Client s3Client, String FILE_NAME, String BUCKET, 
        String CHECKSUM_TAG_KEYNAME, int CHUNK_SIZE) {
        System.out.println("Starting downloading file validation");
        File file = new File("DOWNLOADED_" + FILE_NAME);
        try (OutputStream out = new FileOutputStream(file)) {
            GetObjectAttributesResponse
                    objectAttributes = s3Client.getObjectAttributes(GetObjectAttributesRequest.builder().bucket(BUCKET).key(FILE_NAME)
                    .objectAttributes(ObjectAttributes.OBJECT_PARTS, ObjectAttributes.CHECKSUM).build());
            //Optionally if you need the full object checksum, you can grab a tag you added on the upload
            List<Tag> objectTags = s3Client.getObjectTagging(GetObjectTaggingRequest.builder().bucket(BUCKET).key(FILE_NAME).build()).tagSet();
            String fullObjectChecksum = null;
            for (Tag objectTag : objectTags) {
                if (objectTag.key().equals(CHECKSUM_TAG_KEYNAME)) {
                    fullObjectChecksum = objectTag.value();
                    break;
                }
            }
            MessageDigest sha256FullObject = MessageDigest.getInstance("SHA-256");
            MessageDigest sha256ChecksumOfChecksums = MessageDigest.getInstance("SHA-256");
 
            //If you retrieve the object in parts, and set the ChecksumMode to enabled, the SDK will automatically validate the part checksum
            for (int partNumber = 1; partNumber <= objectAttributes.objectParts().totalPartsCount(); partNumber++) {
                MessageDigest sha256Part = MessageDigest.getInstance("SHA-256");
                ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET).key(FILE_NAME).partNumber(partNumber).checksumMode(ChecksumMode.ENABLED).build());
                GetObjectResponse getObjectResponse = response.response();
                byte[] buffer = new byte[CHUNK_SIZE];
                int read = response.read(buffer);
                while (read != -1) {
                    out.write(buffer, 0, read);
                    sha256FullObject.update(buffer, 0, read);
                    sha256Part.update(buffer, 0, read);
                    read = response.read(buffer);
                }
                byte[] sha256PartBytes = sha256Part.digest();
                sha256ChecksumOfChecksums.update(sha256PartBytes);
                //Optionally, you can do an additional manual validation again the part checksum if needed in addition to the SDK check
                String base64PartChecksum = Base64.getEncoder().encodeToString(sha256PartBytes);
                String base64PartChecksumFromObjectAttributes = objectAttributes.objectParts().parts().get(partNumber - 1).checksumSHA256();
                if (!base64PartChecksum.equals(getObjectResponse.checksumSHA256()) || !base64PartChecksum.equals(base64PartChecksumFromObjectAttributes)) {
                    throw new IOException("Part checksum didn't match for the part");
                }
                System.out.println(partNumber + " " + base64PartChecksum);
            }
            //Before finalizing, do the final checksum validation.
            String base64FullObject = Base64.getEncoder().encodeToString(sha256FullObject.digest());
            String base64ChecksumOfChecksums = Base64.getEncoder().encodeToString(sha256ChecksumOfChecksums.digest());
            if (fullObjectChecksum != null && !fullObjectChecksum.equals(base64FullObject)) {
                throw new IOException("Failed checksum validation for full object");
            }
            System.out.println(fullObjectChecksum);
            String base64ChecksumOfChecksumFromAttributes = objectAttributes.checksum().checksumSHA256();
            if (base64ChecksumOfChecksumFromAttributes != null && !base64ChecksumOfChecksums.equals(base64ChecksumOfChecksumFromAttributes)) {
                throw new IOException("Failed checksum validation for full object checksum of checksums");
            }
            System.out.println(base64ChecksumOfChecksumFromAttributes);
            out.flush();
        } catch (IOException | NoSuchAlgorithmException e) {
            //Cleanup bad file
            file.delete();
            e.printStackTrace();
        }
    }
}
