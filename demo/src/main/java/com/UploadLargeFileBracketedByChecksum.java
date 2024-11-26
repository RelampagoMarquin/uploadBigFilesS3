package com;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class UploadLargeFileBracketedByChecksum {
    public static void uploadLargeFileBracketedByChecksum(S3Client s3Client, String FILE_NAME, String BUCKET, 
        String CHECKSUM_TAG_KEYNAME, int CHUNK_SIZE, String SHA256_FILE_BYTES) {
        System.out.println("Starting uploading file validation");
        File file = new File(FILE_NAME);
        try (InputStream in = new FileInputStream(file)) {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(BUCKET)
                    .key(FILE_NAME)
                    .checksumAlgorithm(ChecksumAlgorithm.SHA256)
                    .build();
            CreateMultipartUploadResponse createdUpload = s3Client.createMultipartUpload(createMultipartUploadRequest);
            List<CompletedPart> completedParts = new ArrayList<CompletedPart>();
            int partNumber = 1;
            byte[] buffer = new byte[CHUNK_SIZE];
            int read = in.read(buffer);
            while (read != -1) {
                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .partNumber(partNumber).uploadId(createdUpload.uploadId()).key(FILE_NAME).bucket(BUCKET).checksumAlgorithm(ChecksumAlgorithm.SHA256).build();
                UploadPartResponse uploadedPart = s3Client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, read)));
                CompletedPart part = CompletedPart.builder().partNumber(partNumber).checksumSHA256(uploadedPart.checksumSHA256()).eTag(uploadedPart.eTag()).build();
                completedParts.add(part);
                sha256.update(buffer, 0, read);
                read = in.read(buffer);
                partNumber++;
            }
            String fullObjectChecksum = Base64.getEncoder().encodeToString(sha256.digest());
            if (!fullObjectChecksum.equals(SHA256_FILE_BYTES)) {
                //Because the SHA256 is uploaded after the part is uploaded; the upload is bracketed and the full object can be fully validated.
                s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(BUCKET).key(FILE_NAME).uploadId(createdUpload.uploadId()).build());
                throw new IOException("Byte mismatch between stored checksum and upload, do not proceed with upload and cleanup");
            }
            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
            CompleteMultipartUploadResponse completedUploadResponse = s3Client.completeMultipartUpload(
                    CompleteMultipartUploadRequest.builder().bucket(BUCKET).key(FILE_NAME).uploadId(createdUpload.uploadId()).multipartUpload(completedMultipartUpload).build());
            Tag checksumTag = Tag.builder().key(CHECKSUM_TAG_KEYNAME).value(fullObjectChecksum).build();
            //Optionally, if you need the full object checksum stored with the file; you could add it as a tag after completion.
            s3Client.putObjectTagging(PutObjectTaggingRequest.builder().bucket(BUCKET).key(FILE_NAME).tagging(Tagging.builder().tagSet(checksumTag).build()).build());
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        GetObjectAttributesResponse
                objectAttributes = s3Client.getObjectAttributes(GetObjectAttributesRequest.builder().bucket(BUCKET).key(FILE_NAME)
                .objectAttributes(ObjectAttributes.OBJECT_PARTS, ObjectAttributes.CHECKSUM).build());
        System.out.println(objectAttributes.objectParts().parts());
        System.out.println(objectAttributes.checksum().checksumSHA256());
    }
}
