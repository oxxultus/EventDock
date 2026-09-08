package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.EventHandler;
import io.github.oxxultus.eventdock.inbox.InboxHandlerRegistry;
import io.github.oxxultus.eventdock.inbox.InboxHandler;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.AnnotatedElementUtils;

final class AnnotatedEventHandlerRegistry implements InboxHandlerRegistry {
  private final Map<Route, InboxHandler> handlers;

  AnnotatedEventHandlerRegistry(List<EventHandler> candidates) {
    var discovered = new LinkedHashMap<Route, InboxHandler>();
    for (EventHandler candidate : candidates) {
      var annotations =
          AnnotatedElementUtils.getMergedRepeatableAnnotations(
              AopUtils.getTargetClass(candidate), EventDockHandler.class, EventDockHandlers.class);
      for (EventDockHandler annotation : annotations) {
        Route route =
            new Route(
                required(annotation.consumerId(), "consumerId"),
                required(annotation.eventType(), "eventType"));
        InboxHandler existing = discovered.putIfAbsent(route, candidate::handle);
        if (existing != null) {
          throw new IllegalStateException(
              "Duplicate EventDock handler for consumerId="
                  + route.consumerId()
                  + ", eventType="
                  + route.eventType());
        }
      }
    }
    handlers = Map.copyOf(discovered);
  }

  @Override
  public InboxHandler get(String consumerId, String eventType) {
    return handlers.get(new Route(consumerId, eventType));
  }

  private static String required(String value, String attribute) {
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("@EventDockHandler " + attribute + " must not be blank");
    }
    return value;
  }

  private record Route(String consumerId, String eventType) {
    private Route {
      Objects.requireNonNull(consumerId);
      Objects.requireNonNull(eventType);
    }
  }
}
