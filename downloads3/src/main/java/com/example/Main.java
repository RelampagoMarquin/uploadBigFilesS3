package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    final ObjectMapper objectMapper = new ObjectMapper();
    final S3Client s3Client = S3Client.builder().build();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        Map<String, Object> response = new HashMap<>();
        String pathParameters = (String) input.get("rawPath");
        if (pathParameters == null || !pathParameters.matches("^/\\d{14}/(NFE|NFCE)/\\d{6}$")) {
            throw new IllegalArgumentException("Parâmetros inválidos: 'cnpj' deve ter 14 dígitos, 'tipo' deve ser 'NFE' ou 'NFCE', e 'data' no formato 'aaaamm'.");
        }

        // Extrai CNPJ, tipo e data
        String[] params = pathParameters.substring(1).split("/");
        String cnpj = params[0];
        String tipo = params[1];
        String data = params[2];
        String prefix = cnpj + "/" + tipo + "/" + data + "/";

        String bucketName = "skysoft-xml-upload-test";

        // Lista os arquivos no prefixo
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response listResponse;
        try {
            listResponse = s3Client.listObjectsV2(listRequest);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao listar objetos do S3: " + e.getMessage(), e);
        }

        if (listResponse.contents().isEmpty()) {
            response.put("code", 404);
            response.put("Mensagem", "Diretorio não contem itens");
        }

        // Cria o arquivo zip temporário
        File tempZipFile;
        try {
            tempZipFile = Files.createTempFile("s3-folder-", ".zip").toFile();
        } catch (Exception e) {
            throw new RuntimeException("Erro ao criar arquivo temporário: " + e.getMessage(), e);
        }

        // Compacta os arquivos listados no zip
        try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(tempZipFile))) {
            for (S3Object s3Object : listResponse.contents()) {
                String key = s3Object.key();
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                try (InputStream s3ObjectStream = s3Client.getObject(getObjectRequest)) {
                    // Adiciona cada arquivo ao zip preservando a estrutura do S3
                    String relativePath = key.substring(prefix.length());
                    zipOut.putNextEntry(new ZipEntry(relativePath));
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = s3ObjectStream.read(buffer)) > 0) {
                        zipOut.write(buffer, 0, bytesRead);
                    }
                    zipOut.closeEntry();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Erro ao compactar arquivos: " + e.getMessage(), e);
        }

        // Codifica o arquivo ZIP em Base64
        byte[] fileContent;
        try {
            fileContent = Files.readAllBytes(tempZipFile.toPath());
        } catch (Exception e) {
            throw new RuntimeException("Erro ao ler o arquivo ZIP: " + e.getMessage(), e);
        }

        String base64File = Base64.getEncoder().encodeToString(fileContent);

        // Retorna o arquivo ZIP em Base64
        response.put("statusCode", 200);
        response.put("body", base64File);
        response.put("isBase64Encoded", true);
        response.put("headers", Map.of("Content-Type", "application/zip"));
        return response;
    }
}