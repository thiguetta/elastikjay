package com.arquivolivre.elastikjay.commons;

import com.arquivolivre.elastikyjay.annotations.Analyzer;
import com.arquivolivre.elastikyjay.annotations.Ignored;
import com.arquivolivre.elastikyjay.annotations.Index;
import com.arquivolivre.elastikyjay.annotations.Nested;
import com.arquivolivre.elastikyjay.annotations.NotAnalyzed;
import com.arquivolivre.elastikyjay.annotations.NotIndexed;
import static com.arquivolivre.elastikjay.commons.Types.isBasicType;
import static com.arquivolivre.elastikjay.commons.Types.isGeneric;
import com.google.gson.Gson;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import org.elasticsearch.common.settings.Settings;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public class IndexManagerImpl implements IndexManager {
    
    private final Client elasticSearchClient;
    private BulkRequestBuilder bulkRequest;
    private final Logger logger = Logger.getLogger(IndexManagerImpl.class);
    
    IndexManagerImpl(Client client) {
        this.elasticSearchClient = client;
    }
    
    public static IndexManagerImpl build(Client client) {
        return new IndexManagerImpl(client);
    }
    
    @Override
    public Client getElasticSearchClient() {
        return elasticSearchClient;
    }
    
    @Override
    public void addToBulk(String id, Object source) {
        if (bulkRequest == null) {
            bulkRequest = elasticSearchClient.prepareBulk();
        }
        if (source != null) {
            String json = new Gson().toJson(source);
            if (!json.isEmpty()) {
                IndexInfo indexInfo = getIndexInfo(source.getClass());
                if (!indexExists(indexInfo.getName())) {
                    createIndex(indexInfo.getName(), indexInfo.getType(), source);
                }
                bulkRequest.add(elasticSearchClient.prepareIndex(indexInfo.getName(), indexInfo.getType(), id).setSource(json));
                logger.info(String.format("Object (id: %s) added to bulk", id));
            }
        } else {
            logger.warn("Attempting to add an empty object, ignoring...");
        }
    }
    
    @Override
    public void executeBulkAdd() {
        if (bulkRequest != null) {
            logger.info("Executing bulk add...");
            BulkResponse actionGet = bulkRequest.execute().actionGet();
            if (actionGet.hasFailures()) {
                logger.error(actionGet.buildFailureMessage());
            } else {
                logger.info("Done!");
            }
            bulkRequest = null;
        }
    }
    
    @Override
    public <A> A get(String id, Class<A> clazz) {
        IndexInfo indexInfo = getIndexInfo(clazz);
        GetResponse response = elasticSearchClient.prepareGet(indexInfo.getName(), indexInfo.getType(), id).execute().actionGet();
        if (response.isSourceEmpty()) {
            String msg = String.format("Object (id: %s) of the type %s was not found in index %s!", id, indexInfo.getName(), indexInfo.getType());
            logger.warn(msg);
            return null;
        }
        return new Gson().fromJson(response.getSourceAsString(), clazz);
    }
    
    private IndexInfo getIndexInfo(Class clazz) {
        try {
            Annotation annotation = clazz.getAnnotation(Index.class);
            String index = (String) annotation.annotationType().getMethod("name").invoke(annotation);
            String type = (String) annotation.annotationType().getMethod("type").invoke(annotation);
            return new IndexInfo(index, type);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            logger.error("Error on retrieving index information.", ex);
        }
        return null;
    }
    
    @Override
    public void putMapping(Object o) {
        IndexInfo indexInfo = getIndexInfo(o.getClass());
        logger.info("puttin map to index " + indexInfo.getName());
        elasticSearchClient.admin().indices().preparePutMapping(indexInfo.getName())
                .setType(indexInfo.getType())
                .setSource(generateMapping(o))
                .execute()
                .actionGet();
    }
    
    @Override
    public boolean indexExists(String index) {
        IndicesExistsResponse response = elasticSearchClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return response.isExists();
    }
    
    @Override
    public void createIndex(String indexName, String indexType, Object source) {
        logger.info(String.format("Generating index %s ...", indexName));
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        if (indexType != null) {
            String settings = generateSettings(source);
            if (settings != null) {
                logger.info("Setting up...");
                createIndexRequest.settings(settings);
            }
        }
        logger.info("Mapping...");
        String mapping = generateMapping(source);
        createIndexRequest.mapping(indexType, mapping);
        try {
            CreateIndexResponse response = elasticSearchClient.admin().indices().create(createIndexRequest).actionGet();
            if (response.isAcknowledged()) {
                logger.info(String.format("Index %s created!", indexName));
            }
        } catch (ElasticsearchException ex) {
            logger.error(String.format("Index %s was not created due some errors.", indexName), ex);
        }
    }
    
    @Override
    public DeleteIndexResponse deleteIndices(String... indices) {
        DeleteIndexRequest request = new DeleteIndexRequest(indices);
        return elasticSearchClient.admin().indices().delete(request).actionGet();
    }
    
    @Override
    public DeleteIndexResponse deleteIndex(String index) {
        return deleteIndices(index);
    }
    
    public String generateMapping(Object obj) {
        Map<Object, Object> typeMap = new TreeMap<>();
        Map<Object, Object> properties = new TreeMap<>();
        IndexInfo indexInfo = getIndexInfo(obj.getClass());
        typeMap.put(indexInfo.getType(), properties);
        try {
            properties.put("properties", getFields(obj.getClass(), false));
        } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            logger.error("Error while parsing mapping.", ex);
            return null;
        }
        return new Gson().toJson(typeMap);
    }
    
    private Map getFields(Class clazz, boolean avoidLoop) throws SecurityException, IllegalArgumentException, IllegalAccessException {
        Field[] declaredFields = clazz.getDeclaredFields();
        Map<Object, Object> fields = new TreeMap<>();
        for (Field field : declaredFields) {
            if (Modifier.isStatic(field.getModifiers()) || field.isAnnotationPresent(Ignored.class)) {
                continue;
            }
            Map<Object, Object> info = new TreeMap<>();
            if (isGeneric(field.getGenericType())) {
                Class clazzType = getInnerType(field);
                if (clazzType == clazz && !avoidLoop) {
                    info.put("properties", getFields(clazzType, true));
                } else if (isBasicType(clazzType.getSimpleName().toLowerCase())) {
                    info.put("type", clazzType.getSimpleName().toLowerCase());
                } else if (!avoidLoop) {
                    info.put("properties", getFields(clazzType, false));
                }
            } else if (isBasicType(field.getType().getSimpleName().toLowerCase())) {
                info.put("type", field.getType().getSimpleName().toLowerCase());
            } else {
                Class aClass = field.getType().getClass();
                if (aClass == clazz && !avoidLoop) {
                    info.put("properties", getFields(aClass, true));
                } else if (!avoidLoop) {
                    info.put("properties", getFields(aClass, false));
                }
            }
            if (field.isAnnotationPresent(NotAnalyzed.class)) {
                info.put("index", "not_analyzed");
            } else if (field.isAnnotationPresent(NotIndexed.class)) {
                info.put("index", "no");
            } else if (field.isAnnotationPresent(Nested.class)) {
                info.put("type", "nested");
            }
            
            if (field.isAnnotationPresent(Analyzer.class)) {
                try {
                    Analyzer annotation = field.getAnnotation(Analyzer.class);
                    String analyzer = (String) annotation.annotationType().getMethod("value").invoke(annotation);
                    info.put("analyzer", analyzer);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
                    logger.error(ex);
                }
            }
            if (!info.isEmpty()) {
                fields.put(field.getName(), info);
            }
            
        }
        return fields;
    }
    
    private Class getInnerType(Field field) {
        Class<?> fieldArgClass = null;
        Type genericType = field.getGenericType();
        if (isGeneric(genericType)) {
            ParameterizedType genericFieldType = (ParameterizedType) genericType;
            Type[] fieldArgTypes = genericFieldType.getActualTypeArguments();
            for (Type fieldArgType : fieldArgTypes) {
                fieldArgClass = (Class<?>) fieldArgType;
            }
        }
        return fieldArgClass;
    }
    
    @Override
    public void updateSettings(Object obj) {
        IndexInfo indexInfo = getIndexInfo(obj.getClass());
        String generateSettings = generateSettings(obj);
        if (generateSettings != null) {
            Settings settings = settingsBuilder().loadFromSource(generateSettings).build();
            logger.info(String.format("Closing index %s", indexInfo.getName()));
            elasticSearchClient.admin().indices().prepareClose(indexInfo.getName()).execute().actionGet();
            logger.info(String.format("Updating index %s settings", indexInfo.getName()));
            elasticSearchClient.admin().indices().prepareUpdateSettings().setSettings(settings).setIndices(indexInfo.getName()).execute().actionGet();
            logger.info(String.format("Opening index %s", indexInfo.getName()));
            elasticSearchClient.admin().indices().prepareOpen(indexInfo.getName()).execute().actionGet();
            
        } else {
            logger.info("No settings to update. Skiping!");
        }
    }
    
    public String generateSettings(Object obj) {
        Index annotation = obj.getClass().getAnnotation(Index.class);
        Class<? extends Annotation> annotationType = annotation.annotationType();
        List<String> ignored = Arrays.asList("name", "type");
        Map<String, Object> result = null;
        for (Method m : annotationType.getDeclaredMethods()) {
            if (ignored.contains(m.getName())) {
                continue;
            }
            Object resultMap = null;
            try {
                Object res = m.invoke(annotation);
                resultMap = generateMap(res);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                logger.error("An error occurred while generating settings: ", ex);
            }
            if (resultMap != null) {
                if (result == null) {
                    result = new TreeMap<>();
                }
                result.put(m.getName(), resultMap);
            }
        }
        return new Gson().toJson(result);
    }
    
    private Object generateMap(Object annotation) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class<? extends Annotation> annotationType = ((Annotation) annotation).annotationType();
        Map<String, Object> result = new TreeMap<>();
        Map<String, Object> inner = new TreeMap<>();
        for (Method m : annotationType.getDeclaredMethods()) {
            Object res = m.invoke(annotation);
            if (m.getName().equals("value") && res.equals("null")) {
                return null;
            }
            if (m.getName().equals("name")) {
                result.put(res.toString(), inner);
            } else {
                Class<? extends Object> aClass = res.getClass();
                Class<?>[] interfaces = aClass.getInterfaces();
                if (interfaces.length > 0 && interfaces[0].isAnnotation()) {
                    inner.put(m.getName(), generateMap(res));
                } else if (aClass.isArray() && aClass.getComponentType().isAnnotation()) {
                    Object[] unknwArr = (Object[]) res;
                    Map<String, Object> arr = new TreeMap<>();
                    for (Object obj : unknwArr) {
                        arr.putAll((Map<String, Object>) generateMap(obj));
                    }
                    inner.put(m.getName(), arr);
                } else {
                    if (!isEmpty(res)) {
                        if (aClass.isEnum()) {
                            inner.put(m.getName(), res.toString());
                        } else {
                            inner.put(m.getName(), res);
                        }
                    }
                }
            }
        }
        if (!result.containsValue(inner)) {
            for (String key : inner.keySet()) {
                result.put(key, inner.get(key));
            }
        }
        return result;
    }
    
    private boolean isEmpty(Object obj) {
        return obj == null
                || obj.equals("")
                || obj.equals("null")
                || (obj.getClass().isArray() ? ((Object[]) obj).length == 0 : false);
    }
    
}
