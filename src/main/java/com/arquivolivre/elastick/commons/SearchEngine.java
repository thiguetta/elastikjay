package com.arquivolivre.elastick.commons;

import org.elasticsearch.action.bulk.BulkResponse;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public interface SearchEngine {

    /**
     * Inicia um novo BulkRequestBuilder caso ainda não exista e adiciona um
     * item.
     *
     * @param id id unico no indice
     * @param source dado a ser inserido no indice
     */
    void add(String id, Object source);

    /**
     * executa a requisicão em massa do BulkRequest criado após a insercão limpa
     * o bulkRequest.
     *
     * @return retorna objeto Iterable contento as resposta de cada inserçao
     */
    BulkResponse performAction();

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
    void putMap(Object map);

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
     */
    void createIndex(String index);

    void injectSetting(Object obj);
}
