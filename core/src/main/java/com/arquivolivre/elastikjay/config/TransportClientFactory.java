package com.arquivolivre.elastikjay.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public class TransportClientFactory {

    private TransportClient transport;

    public TransportClient getTransport(Settings settings) {
        if (settings == null) {
            return getTransport();
        }
        if (transport == null) {
            transport = new TransportClient(settings);
        }
        return transport;
    }

    public TransportClient getTransport() {
        if (transport == null) {
            transport = new TransportClient();
        }
        return transport;
    }

}
