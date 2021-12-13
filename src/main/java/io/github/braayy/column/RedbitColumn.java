package io.github.braayy.column;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RedbitColumn {

    String sqlType();
    String name() default "";
    String defaultValue() default "";
    int length() default 0;
    boolean idColumn() default false;
    boolean autoIncrement() default false;
    boolean nullable() default false;

}
