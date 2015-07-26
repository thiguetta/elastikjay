package com.arquivolivre.elastikjay.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public class Configuration {
    
    protected TransportClientFactory transportFactory;
    private final String CLUSTER_NAME_KEY = "elasticsearch.cluster.name";
    private final String CLUSTER_NODES_KEY = "elasticsearch.cluster.nodes";
    private final String CLUSTER_PORTS_KEY = "elasticsearch.cluster.ports";
    private final String DEFAULT_PORT_KEY = "elasticsearch.port";
    private final String DEFAULT_CLUSTER_NODES_KEY = "elasticsearch.nodes";
    private final String DEFAULT_NODE_KEY = "elasticsearch.node";
    private Properties properties;
    private final Logger logger = Logger.getLogger(Configuration.class);
    
    public Client elasticSearchClient() {
        properties = getResources();
        Settings settings = null;
        String[] hosts = null;
        String[] ports = null;
        String host = null;
        String port = "9300"; //porta padrao
        //verifica configuraçao de cluster ou padrao
        if (properties.containsKey(CLUSTER_NAME_KEY)) {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", properties.get(CLUSTER_NAME_KEY)).build();
            hosts = properties.getProperty(CLUSTER_NODES_KEY).split(",");
        } else if (properties.containsKey(DEFAULT_CLUSTER_NODES_KEY)) {
            hosts = properties.getProperty(DEFAULT_CLUSTER_NODES_KEY).split(",");
        } else {
            host = properties.getProperty(DEFAULT_NODE_KEY);
        }

        //verifica portas padrão
        if (properties.containsKey(CLUSTER_PORTS_KEY)) {
            ports = properties.getProperty(CLUSTER_PORTS_KEY).split(",");
        } else {
            port = properties.getProperty(DEFAULT_PORT_KEY);
        }
        
        if (hosts != null) {
            InetSocketTransportAddress[] addrs = new InetSocketTransportAddress[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                if (ports != null) {
                    port = ports[i];
                }
                addrs[i] = new InetSocketTransportAddress(hosts[i].trim(), Integer.parseInt(port.trim()));
            }
            return transportFactory.getTransport(settings).addTransportAddresses(addrs);
        }
        
        InetSocketTransportAddress addr = new InetSocketTransportAddress(host, Integer.parseInt(port.trim()));
        return transportFactory.getTransport().addTransportAddress(addr);
        
    }
    
    private Properties getResources() {
        //TODO:default configuration file name
        Properties prop = new Properties();
        try (InputStream in = getClass().getResourceAsStream("es.properties")) {
            prop.load(in);
        } catch (IOException ex) {
            logger.error(null, ex);
        }
        return prop;
    }
    
}
