package com.example;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class LowLevelMultipartUpload {

    public static void main(String[] args) throws IOException {
        Region clientRegion = Region.US_EAST_1;  // Substitua pela sua região
        String bucketName = "*** Bucket name ***";
        String keyName = "*** Key name ***";
        String filePath = "*** Path to file to upload ***";

        File file = new File(filePath);
        long contentLength = file.length();
        long partSize = 5 * 1024 * 1024; // Define o tamanho do bloco para 5 MB

        try (S3Client s3Client = S3Client.builder()
                .region(clientRegion)
                .credentialsProvider(ProfileCredentialsProvider.create())  // Se estiver usando as credenciais do perfil
                .build()) {

            // Crie uma lista de ETag para armazenar os ETags de cada parte do upload
            List<CompletedPart> partETags = new ArrayList<>();

            // Inicia o upload multipart.
            CreateMultipartUploadRequest initRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            CreateMultipartUploadResponse initResponse = s3Client.createMultipartUpload(initRequest);

            // Realiza o upload das partes do arquivo
            long filePosition = 0;
            int partNumber = 1;

            try (FileInputStream fis = new FileInputStream(file)) {
                FileChannel channel = fis.getChannel();

                while (filePosition < contentLength) {
                    // Ajuste o tamanho da parte, caso o último pedaço seja menor que 5 MB
                    long remaining = contentLength - filePosition;
                    partSize = Math.min(partSize, remaining);

                    // Cria um buffer de bytes para a parte do arquivo a ser carregada
                    byte[] data = new byte[(int) partSize];
                    channel.read(java.nio.ByteBuffer.wrap(data), filePosition);

                    // Solicitação para carregar a parte (não é necessário partSize no Builder)
                    UploadPartRequest uploadRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(keyName)
                            .uploadId(initResponse.uploadId())
                            .partNumber(partNumber++)
                            .build();

                    // Realiza o upload da parte
                    UploadPartResponse uploadResult = s3Client.uploadPart(uploadRequest, RequestBody.fromBytes(data));

                    // Adiciona o ETag da parte à lista
                    CompletedPart completedPart = CompletedPart.builder()
                            .partNumber(partNumber - 1)  // Ajuste no número da parte
                            .eTag(uploadResult.eTag())
                            .build();
                    partETags.add(completedPart);

                    // Atualiza a posição do arquivo
                    filePosition += partSize;
                }
            }

            // Finaliza o upload multipart.
            CompleteMultipartUploadRequest compRequest = CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .uploadId(initResponse.uploadId())
                    .multipartUpload(CompletedMultipartUpload.builder()
                            .parts(partETags)
                            .build())
                    .build();

            s3Client.completeMultipartUpload(compRequest);

            System.out.println("Upload multipart finalizado com sucesso!");

        } catch (S3Exception e) {
            // Caso um erro S3 ocorra, ele será tratado aqui
            e.printStackTrace();
        }
    }
}
