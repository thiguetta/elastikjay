package com.arquivolivre.elastikjay.commons;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public interface IndexManager {

    /**
     * Inicia um novo BulkRequestBuilder caso ainda não exista e adiciona um
     * item.
     *
     * @param id id unico no indice
     * @param source dado a ser inserido no indice
     */
    void addToBulk(String id, Object source);

    /**
     * executa a requisicão em massa do BulkRequest criado após a insercão limpa
     * o bulkRequest.
     *
     */
    void executeBulkAdd();

    /**
     * Recupera um objeto do indice a partir do id
     *
     * @param <A> tipo da classe que será retornada
     * @param id id do objeto no indice
     * @param clazz classe que será retornada
     * @return retorna um objeto do tipo clazz
     */
    <A> A get(String id, Class<A> clazz);

    /**
     * insere o mapa no devido indice.
     *
     * @param map objeto correspondente ao mapa a ser inserido
     */
    void putMapping(Object map);

    /**
     * verifica se o indice já existe no elasticsearch.
     *
     * @param index nome do indice
     * @return retorna true caso exista, false caso contrário
     */
    boolean indexExists(String index);

    /**
     * cria o indice no elasticsearch com o dado nome.
     *
     * @param index nome do indice a ser criado
     * @param type tipo
     * @param source
     */
    void createIndex(String index, String type, Object source);

    DeleteIndexResponse deleteIndices(String... indices);

    DeleteIndexResponse deleteIndex(String index);

    /**
     *
     * @param source
     */
    void updateSettings(Object source);

    Client getElasticSearchClient();

}
