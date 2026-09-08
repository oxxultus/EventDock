package io.github.oxxultus.eventdock.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class EventDockPropertiesTest {
  @Test
  void producerOnlyDefaultsAreValid() {
    assertDoesNotThrow(new EventDockProperties()::validate);
  }

  @Test
  void enabledInboxRequiresIdentityAndTopics() {
    var properties = new EventDockProperties();
    properties.getInbox().setEnabled(true);

    assertThrows(IllegalStateException.class, properties::validate);
  }
}
