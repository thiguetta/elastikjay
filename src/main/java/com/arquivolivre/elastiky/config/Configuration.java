package com.arquivolivre.elastiky.config;

import java.io.InputStream;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
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
    private Map<String, String> environment;

    public Client elasticSearchClient() {
        environment = getResources();
        Settings settings = null;
        String[] hosts = null;
        String[] ports = null;
        String host = null;
        String port = "9300"; //porta padrao
        //verifica configuraçao de cluster ou padrao
        if (environment.containsKey(CLUSTER_NAME_KEY)) {
            settings = ImmutableSettings.settingsBuilder().put("cluster.name", environment.get(CLUSTER_NAME_KEY)).build();
            hosts = environment.get(CLUSTER_NODES_KEY).split(",");
        } else if (environment.containsKey(DEFAULT_CLUSTER_NODES_KEY)) {
            hosts = environment.get(DEFAULT_CLUSTER_NODES_KEY).split(",");
        } else {
            host = environment.get(DEFAULT_NODE_KEY);
        }

        //verifica portas padrão
        if (environment.containsKey(CLUSTER_PORTS_KEY)) {
            ports = environment.get(CLUSTER_PORTS_KEY).split(",");
        } else {
            port = environment.get(DEFAULT_PORT_KEY);
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

    private Map<String, String> getResources() {
        //TODO:default configuration file
        InputStream resourceAsStream = this.getClass().getResourceAsStream(CLUSTER_NAME_KEY);
        Scanner sc = new Scanner(resourceAsStream);
        Map<String, String> result = new TreeMap<>();
        while (sc.hasNext()) {
            String nextLine = sc.nextLine();
            if (nextLine.startsWith("#")) {
                continue;
            }
            String[] split = nextLine.split("=");
            if (split.length > 1) {
                result.put(split[0], split[1]);
            }
        }
        return result;
    }

}
