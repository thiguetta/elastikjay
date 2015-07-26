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
@Target(ElementType.TYPE)
public @interface Filter {

    String name();

    FilterType type() default FilterType.NONE;

    String stopwords() default "";

    String language() default "";

    String[] keywords() default {};

    String[] synonyms() default {};

}
