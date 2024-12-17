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

        // instacia o S3 da amazon
        final S3Client s3Client = S3Client.builder().build();

        String body = input.get("body").toString();
        // Verifica se existe um Json na requisição
        Map<String, Object> bodyMap = new HashMap<>();
        try {
            System.out.println(body);
            bodyMap = objectMapper.readValue(body, Map.class);
        } catch (Exception error) {
            throw new RuntimeException("Error parsing JSON body: " + error.getMessage(), error);
        }

        // Variaveis do Json
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
            throw new RuntimeJsonMappingException("Adicione o status (AUTORIZADA, CANCELADA, INUTILIZADA OU CONTIGENCIA)");
        }

        String data = (String) bodyMap.get("data");

        if (data == null) {
            throw new RuntimeJsonMappingException("Adicione a data da Emissão da nota");
        }

        String numNota = (String) bodyMap.get("numNota");

        if (numNota == null) {
            throw new RuntimeJsonMappingException("Adicione o numero da nota");
        }

        String file = (String) bodyMap.get("file");

        if (file == null) {
            throw new RuntimeJsonMappingException("Adicione o arquivo xml da nota");
        }

        // Decodificação caso o 'file' esteja em base64
        byte[] fileBytes = new byte[0];
        if (file != null) {
            fileBytes = java.util.Base64.getDecoder().decode(file); // Caso o arquivo esteja em base64
        }

        // Tenta fazer a conexão com o S3
        try {
            // fazer uma chamada para o dinamo DB e salva os dados que não sejam o xml

            // cria uma requisição para salvar na S3
            String fileName = numNota + ".xml"; 
            String dir = cnpj + "/" + tipo + "/" + data + "/" + status + "/" + fileName;
            PutObjectRequest request = PutObjectRequest.builder()
                .bucket("skysoft-xml-upload-test")
                .key(dir)
                .build();
            
                s3Client.putObject(request, RequestBody.fromString(file));
        } catch (Exception e) {
            throw new RuntimeException("Erro ao salvar dado no S3: " + e.getMessage());
        }

        Map<String, String> response = new HashMap<>();

        response.put("code", "XML SALVO");

        return response;
    };

 
}
