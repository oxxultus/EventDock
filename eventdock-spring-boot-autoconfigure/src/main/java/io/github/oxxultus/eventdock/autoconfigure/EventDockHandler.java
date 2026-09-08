package io.github.oxxultus.eventdock.autoconfigure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Declares the consumer and event type handled by an {@code EventHandler} bean. */
@Inherited
@Repeatable(EventDockHandlers.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EventDockHandler {
  /**
   * Logical consumer ID used for handler routing and Inbox deduplication.
   *
   * @return consumer ID configured by the matching binding or route
   */
  String consumerId();

  /**
   * EventDock event type header value handled by the bean.
   *
   * @return exact, case-sensitive event type
   */
  String eventType();
}
