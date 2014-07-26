package com.arquivolivre.elastick.commons;

import com.arquivolivre.elastiky.annotations.Analyzer;
import com.arquivolivre.elastiky.annotations.Ignored;
import com.arquivolivre.elastiky.annotations.Index;
import com.arquivolivre.elastiky.annotations.Nested;
import com.arquivolivre.elastiky.annotations.NotAnalyzed;
import com.arquivolivre.elastiky.annotations.NotIndexed;
import com.google.gson.Gson;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
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
public class SearchEngineImpl implements SearchEngine {

    private final Client client;
    private BulkRequestBuilder bulkRequest;
    private final Logger logger = Logger.getLogger(SearchEngineImpl.class);
    private final List<String> list;

    SearchEngineImpl(Client client) {
        this.client = client;
        list = Arrays.asList("string", "integer", "long", "float", "double", "boolean");
    }

    public static SearchEngineImpl build(Client client) {
        return new SearchEngineImpl(client);
    }

    public Client getClient() {
        return client;
    }

    @Override
    public void add(String id, Object source) {
        if (bulkRequest == null) {
            bulkRequest = client.prepareBulk();
        }
        if (source != null) {
            String json = new Gson().toJson(source);
            if (!json.isEmpty()) {
                try {
                    Annotation annotation = source.getClass().getAnnotation(Index.class);
                    String index = (String) annotation.annotationType().getMethod("name").invoke(annotation);
                    String type = (String) annotation.annotationType().getMethod("type").invoke(annotation);
                    if (!indexExists(index)) {
                        createIndex(index);
                        putMap(source);
                    }
                    bulkRequest.add(client.prepareIndex(index, type, id).setSource(json));
                    logger.info("Added to bulk id " + id);
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
                    Logger.getLogger(SearchEngineImpl.class.getName()).error(ex);
                }
            }
        } else {
            logger.warn("Trying to add an empty source, ignoring...");
        }
    }

    @Override
    public BulkResponse performAction() {
        BulkResponse actionGet = null;

        if (bulkRequest != null) {
            logger.info("Executing bulk index add...");
            actionGet = bulkRequest.execute().actionGet();
            if (actionGet.hasFailures()) {
                logger.error(actionGet.buildFailureMessage());
            } else {
                logger.info("SUCCESS!");
            }
            bulkRequest = null;
        }
        return actionGet;
    }

    @Override
    public <A> A get(String id, Class<A> clazz) {
        GetResponse actionGet = null;
        try {
            Annotation annotation = clazz.getAnnotation(Index.class);
            String index = (String) annotation.annotationType().getMethod("name").invoke(annotation);
            String type = (String) annotation.annotationType().getMethod("type").invoke(annotation);
            actionGet = client.prepareGet(index, type, id).execute().actionGet();
            if (actionGet.isSourceEmpty()) {
                logger.info("Object type: " + type + " id: " + id + " not found in " + index + "!");
                return null;
            }
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            Logger.getLogger(SearchEngineImpl.class.getName()).error(ex);
        }
        return new Gson().fromJson(actionGet.getSourceAsString(), clazz);
    }

    @Override
    public void putMap(Object o) {
        Annotation annotation = o.getClass().getAnnotation(Index.class);
        try {
            String index = (String) annotation.annotationType().getMethod("name").invoke(annotation);
            String type = (String) annotation.annotationType().getMethod("type").invoke(annotation);
            logger.info("puttin map to index " + index);
            PutMappingResponse actionGet = client.admin().indices().preparePutMapping(index)
                    .setType(type)
                    .setSource(generateMapping(o))
                    .execute()
                    .actionGet();
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException ex) {
            Logger.getLogger(SearchEngineImpl.class).error(null, ex);
        }
    }

    @Override
    public boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        return actionGet.isExists();
    }

    @Override
    public void createIndex(String index) {
        logger.info("generating index " + index);
        client.admin().indices().create(new CreateIndexRequest(index)).actionGet();
    }

    public String generateMapping(Object obj) throws NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Map<Object, Object> typeMap = new TreeMap<>();
        Map<Object, Object> properties = new TreeMap<>();
        Annotation annotation = obj.getClass().getAnnotation(Index.class);
        String type = (String) annotation.annotationType().getMethod("type").invoke(annotation);
        typeMap.put(type, properties);
        properties.put("properties", getFields(obj.getClass(), false));
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
                    Logger.getLogger(SearchEngineImpl.class.getName()).error(ex);
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

    private boolean isGeneric(Type type) {
        return type instanceof ParameterizedType;
    }

    private boolean isBasicType(String name) {
        return list.contains(name);
    }

    @Override
    public void injectSetting(Object obj) {
        Annotation annotation = obj.getClass().getAnnotation(Index.class);
        try {
            String index = (String) annotation.annotationType().getMethod("name").invoke(annotation);
            Settings settings = settingsBuilder().loadFromSource(generateSettings(obj)).build();
            logger.info("closing index " + index);
            client.admin().indices().prepareClose(index).execute().actionGet();
            logger.info("updating index " + index + " settings");
            client.admin().indices().prepareUpdateSettings().setSettings(settings).setIndices(index).execute().actionGet();
            logger.info("opening index " + index);
            client.admin().indices().prepareOpen(index).execute().actionGet();
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            java.util.logging.Logger.getLogger(SearchEngineImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private String generateSettings(Object obj) throws NoSuchMethodException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Index annotation = obj.getClass().getAnnotation(Index.class);

        String[] synonyms = (String[]) annotation.annotationType().getMethod("synonyms").invoke(annotation);
        Map<String, Object> analysis = new TreeMap<>();
        if (synonyms.length > 0 && !synonyms[0].equals("")) {
            Map<String, Object> synonym = new TreeMap<>();
            Map<String, Object> filterField = new TreeMap<>();
            Map<String, Object> analyzerType = new TreeMap<>();
            Map<String, Object> analyzerOptions = new TreeMap<>();
            Map<String, Object> opt = new TreeMap<>();
            analyzerOptions.put("filter", new String[]{"synonym"});
            //TODO: aqui deve ser o nome, creio que sera necessario uma maneira pratica de dar esse nome ao analyzer
            analyzerType.put("synonymAnalyzer", analyzerOptions);
            synonym.put("type", "synonym");
            synonym.put("synonyms", synonyms);
            filterField.put("synonym", synonym);
            opt.put("analyzer", analyzerType);
            opt.put("filter", filterField);
            analysis.put("analysis", opt);
        }
        return new Gson().toJson(analysis);
    }

}
