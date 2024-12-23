package com.teste;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, String>>{

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, String> handleRequest(Map<String, Object> input, Context context) {
        final ObjectMapper objectMapper = new ObjectMapper();

        // Instancia o cliente S3 da AWS
        final S3Client s3Client = S3Client.builder().build();

        // Determina se a requisição é multipart
        boolean isMultipart = input.get("headers").toString().contains("multipart/form-data");

        Map<String, Object> bodyMap = new HashMap<>();
        byte[] fileBytes = null;

        try {
            if (isMultipart) {
                try {
                    // Obtém o corpo da requisição como InputStream
                    byte[] bodyBytes = ((String) input.get("body")).getBytes();
                    String contentType = ((Map<String, String>) input.get("headers")).get("Content-Type");
            
                    if (!contentType.startsWith("multipart/form-data")) {
                        throw new RuntimeException("Requisição não contém Content-Type válido.");
                    }
            
                    // Parse do boundary do Content-Type
                    String boundary = contentType.split("boundary=")[1];
                    String delimiter = "--" + boundary;
            
                    // Divida o corpo pelo delimitador
                    String[] parts = new String(bodyBytes).split(delimiter);
                    for (String part : parts) {
                        if (part.trim().isEmpty() || part.equals("--")) {
                            continue;
                        }
            
                        // Verifica se a parte é campo de formulário ou arquivo
                        if (part.contains("Content-Disposition: form-data;")) {
                            String[] lines = part.split("\\R", 3); // Divide as linhas no formato [headers, vazio, conteúdo]
                            if (lines.length < 3) continue;
            
                            String headers = lines[0];
                            String content = lines[2]; // Dados reais
            
                            if (headers.contains("filename")) {
                                // Processar como arquivo
                                String filename = headers.split("filename=\"")[1].split("\"")[0];
                                fileBytes = content.trim().getBytes();
                                System.out.println("Arquivo recebido: " + filename);
                            } else {
                                // Processar como campo de formulário
                                String name = headers.split("name=\"")[1].split("\"")[0];
                                bodyMap.put(name, content.trim());
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Erro ao processar multipart/form-data manualmente: " + e.getMessage(), e);
                }       
            } else {
                // Processa JSON
                String body = input.get("body").toString();
                bodyMap = objectMapper.readValue(body, Map.class);
            }
        } catch (Exception error) {
            throw new RuntimeException("Error parsing request body: " + error.getMessage(), error);
        }

        // Valida campos obrigatórios
        String cnpj = (String) bodyMap.get("cnpj");
        if (cnpj == null) {
            throw new RuntimeJsonMappingException("Adicione o cnpj");
        }

        String tipo = (String) bodyMap.get("tipo");
        if (tipo == null) {
            throw new RuntimeJsonMappingException("Adicione o tipo (NFE ou NFCE)");
        }

        String status = (String) bodyMap.get("status");
        if (status == null) {
            throw new RuntimeJsonMappingException("Adicione o status (AUTORIZADA, CANCELADA, INUTILIZADA OU CONTINGENCIA)");
        }

        String data = (String) bodyMap.get("data");
        if (data == null) {
            throw new RuntimeJsonMappingException("Adicione a data da emissão da nota");
        }

        String numNota = (String) bodyMap.get("numNota");
        if (numNota == null) {
            throw new RuntimeJsonMappingException("Adicione o número da nota");
        }

        if (fileBytes == null) {
            String file = (String) bodyMap.get("file");
            if (file == null) {
                throw new RuntimeJsonMappingException("Adicione o arquivo xml da nota");
            }
            fileBytes = java.util.Base64.getDecoder().decode(file);
        }

        // Salva o arquivo no S3
        try {
            String fileName = numNota + ".xml";
            String dir = cnpj + "/" + tipo + "/" + data + "/" + status + "/" + fileName;
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket("skysoft-xml-upload-test")
                .key(dir)
                .build();

            s3Client.putObject(request, RequestBody.fromBytes(fileBytes));
        } catch (Exception e) {
            throw new RuntimeException("Erro ao salvar dado no S3: " + e.getMessage(), e);
        }

        Map<String, String> response = new HashMap<>();
        response.put("code", "XML SALVO");

        return response;
    }
}
