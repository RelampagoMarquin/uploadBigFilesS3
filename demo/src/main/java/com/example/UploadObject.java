package com.example;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.regions.Region;
import java.io.File;
import java.io.IOException;

public class UploadObject {

    public static void main(String[] args) throws IOException {
        Region clientRegion = Region.US_EAST_1;  // Substitua com sua região
        String bucketName = "*** Bucket name ***";
        String stringObjKeyName = "*** String object key name ***";
        String fileObjKeyName = "*** File object key name ***";
        String fileName = "*** Path to file to upload ***";

        try {
            // Criação do cliente S3
            S3Client s3Client = S3Client.builder()
                    .region(clientRegion)
                    .build();

            // Verificando se o arquivo já existe no bucket
            if (doesFileExist(s3Client, bucketName, fileObjKeyName)) {
                System.out.println("O arquivo com o nome '" + fileObjKeyName + "' já existe no bucket.");
            } else {
                System.out.println("O arquivo com o nome '" + fileObjKeyName + "' não existe no bucket.");
                
                // Fazendo upload de uma string (exemplo)
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(stringObjKeyName)
                        .build();
                s3Client.putObject(putObjectRequest, RequestBody.fromString("Uploaded String Object"));

                // Fazendo upload de um arquivo
                File file = new File(fileName);
                putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(fileObjKeyName)
                        .build();
                s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
                System.out.println("Arquivo enviado com sucesso!");
            }
        } catch (S3Exception e) {
            // Exceções específicas do S3
            e.printStackTrace();
        } catch (Exception e) {
            // Exceções gerais do SDK
            e.printStackTrace();
        }
    }

    // Verifica se o arquivo existe usando o headObject
    private static boolean doesFileExist(S3Client s3Client, String bucketName, String fileName) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .build();
            s3Client.headObject(headObjectRequest);  // Se o arquivo existir, não gerará exceção
            return true;
        } catch (S3Exception e) {
            // Se o erro for 404, significa que o arquivo não existe
            if (e.statusCode() == 404) {
                return false;
            }
            // Lançar exceção caso seja outro tipo de erro
            throw e;
        }
    }
}

