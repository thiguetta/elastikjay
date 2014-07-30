package com.arquivolivre.elastiky.commons;

import com.arquivolivre.elastiky.annotations.Analyzer;
import com.arquivolivre.elastiky.annotations.Ignored;
import com.arquivolivre.elastiky.annotations.Index;
import com.arquivolivre.elastiky.annotations.Nested;
import com.arquivolivre.elastiky.annotations.NotAnalyzed;
import com.arquivolivre.elastiky.annotations.NotIndexed;
import static com.arquivolivre.elastiky.commons.Types.isBasicType;
import static com.arquivolivre.elastiky.commons.Types.isGeneric;
import com.google.gson.Gson;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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
                    createIndex(indexInfo.getName(), source);
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
            logger.info(msg);
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
            Logger.getLogger(IndexManagerImpl.class.getName()).error(ex);
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
        IndicesExistsResponse actionGet = elasticSearchClient.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return actionGet.isExists();
    }

    @Override
    public void createIndex(String index, Object source) {
        logger.info(String.format("Generating index %s", index));
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        IndexInfo indexInfo = getIndexInfo(source.getClass());
        String settings = generateSettings(source);
        if (settings != null) {
            createIndexRequest.settings(settings);
        }
        String mapping = generateMapping(source);
        createIndexRequest.mapping(indexInfo.getType(), mapping);
        elasticSearchClient.admin().indices().create(createIndexRequest).actionGet();
    }

    public String generateMapping(Object obj) {
        Map<Object, Object> typeMap = new TreeMap<>();
        Map<Object, Object> properties = new TreeMap<>();
        IndexInfo indexInfo = getIndexInfo(obj.getClass());
        typeMap.put(indexInfo.getType(), properties);
        try {
            properties.put("properties", getFields(obj.getClass(), false));
        } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(IndexManagerImpl.class).error(null, ex);
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
                    Logger.getLogger(IndexManagerImpl.class.getName()).error(ex);
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

    private String generateSettings(Object obj) {
        try {
            Index annotation = obj.getClass().getAnnotation(Index.class);
            String[] synonyms = (String[]) annotation.annotationType().getMethod("synonyms").invoke(annotation);
            if (synonyms.length > 0 && !synonyms[0].equals("")) {
                Map<String, Object> analysis = new TreeMap<>();
                Map<String, Object> synonym = new TreeMap<>();
                Map<String, Object> filterField = new TreeMap<>();
                Map<String, Object> analyzerType = new TreeMap<>();
                Map<String, Object> analyzerOptions = new TreeMap<>();
                Map<String, Object> opt = new TreeMap<>();
                analyzerOptions.put("filter", new String[]{"synonym"});
                analyzerOptions.put("tokenizer", "whitespace");
                analyzerType.put("synonym", analyzerOptions);
                synonym.put("type", "synonym");
                synonym.put("synonyms", synonyms);
                filterField.put("synonym", synonym);
                opt.put("analyzer", analyzerType);
                opt.put("filter", filterField);
                analysis.put("analysis", opt);
                return new Gson().toJson(analysis);
            }
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            Logger.getLogger(IndexManagerImpl.class).error(null, ex);
        }
        return null;
    }

}
