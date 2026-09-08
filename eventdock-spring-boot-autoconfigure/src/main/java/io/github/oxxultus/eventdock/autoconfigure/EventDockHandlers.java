package io.github.oxxultus.eventdock.autoconfigure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Container annotation for repeatable {@link EventDockHandler} declarations. */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EventDockHandlers {
  /**
   * Returns the handler mappings declared on one class.
   *
   * @return repeated handler annotations
   */
  EventDockHandler[] value();
}
