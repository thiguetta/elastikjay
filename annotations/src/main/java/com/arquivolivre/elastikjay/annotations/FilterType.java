package com.arquivolivre.elastikjay.annotations;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public enum FilterType {

    STOP,
    KEYWORD_MARKER,
    STEMMER,
    SYNONYM,
    NONE;

    @Override
    public String toString() {
        return this.name().toLowerCase();
    }
}
