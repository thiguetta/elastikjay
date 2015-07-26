/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arquivolivre.elastikjay.commons;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
public class Types {

    private static final List<String> types = Arrays.asList("string", "integer", "long", "float", "double", "boolean");

    public static boolean isGeneric(Type type) {
        return type instanceof ParameterizedType;
    }

    public static boolean isBasicType(String name) {
        return types.contains(name);
    }

}
