package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;

public class ValidateExistingFileAgainstS3Checksum {
    public static void validateExistingFileAgainstS3Checksum(S3Client s3Client, String FILE_NAME, String BUCKET, 
    String CHECKSUM_TAG_KEYNAME, int CHUNK_SIZE) {
        System.out.println("Starting existing file validation");
        File file = new File("DOWNLOADED_" + FILE_NAME);
        GetObjectAttributesResponse
                objectAttributes = s3Client.getObjectAttributes(GetObjectAttributesRequest.builder().bucket(BUCKET).key(FILE_NAME)
                .objectAttributes(ObjectAttributes.OBJECT_PARTS, ObjectAttributes.CHECKSUM).build());
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest sha256ChecksumOfChecksums = MessageDigest.getInstance("SHA-256");
            MessageDigest sha256Part = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[CHUNK_SIZE];
            int currentPart = 0;
            int partBreak = objectAttributes.objectParts().parts().get(currentPart).size();
            int totalRead = 0;
            int read = in.read(buffer);
            while (read != -1) {
                totalRead += read;
                if (totalRead >= partBreak) {
                    int difference = totalRead - partBreak;
                    byte[] partChecksum;
                    if (totalRead != partBreak) {
                        sha256Part.update(buffer, 0, read - difference);
                        partChecksum = sha256Part.digest();
                        sha256ChecksumOfChecksums.update(partChecksum);
                        sha256Part.reset();
                        sha256Part.update(buffer, read - difference, difference);
                    } else {
                        sha256Part.update(buffer, 0, read);
                        partChecksum = sha256Part.digest();
                        sha256ChecksumOfChecksums.update(partChecksum);
                        sha256Part.reset();
                    }
                    String base64PartChecksum = Base64.getEncoder().encodeToString(partChecksum);
                    if (!base64PartChecksum.equals(objectAttributes.objectParts().parts().get(currentPart).checksumSHA256())) {
                        throw new IOException("Part checksum didn't match S3");
                    }
                    currentPart++;
                    System.out.println(currentPart + " " + base64PartChecksum);
                    if (currentPart < objectAttributes.objectParts().totalPartsCount()) {
                        partBreak += objectAttributes.objectParts().parts().get(currentPart - 1).size();
                    }
                } else {
                    sha256Part.update(buffer, 0, read);
                }
                read = in.read(buffer);
            }
            if (currentPart != objectAttributes.objectParts().totalPartsCount()) {
                currentPart++;
                byte[] partChecksum = sha256Part.digest();
                sha256ChecksumOfChecksums.update(partChecksum);
                String base64PartChecksum = Base64.getEncoder().encodeToString(partChecksum);
                System.out.println(currentPart + " " + base64PartChecksum);
            }
 
            String base64CalculatedChecksumOfChecksums = Base64.getEncoder().encodeToString(sha256ChecksumOfChecksums.digest());
            System.out.println(base64CalculatedChecksumOfChecksums);
            System.out.println(objectAttributes.checksum().checksumSHA256());
            if (!base64CalculatedChecksumOfChecksums.equals(objectAttributes.checksum().checksumSHA256())) {
                throw new IOException("Full object checksum of checksums don't match S3");
            }
 
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
