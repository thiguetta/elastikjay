package com.arquivolivre.elastikjay.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author Thiago da Silva Gonzaga <thiagosg@sjrp.unesp.br>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface Analysis {

    String value() default "";

    Filter[] filter() default @Filter(name = "null");

    Analyzer analyzer() default @Analyzer(name = "null");
}
