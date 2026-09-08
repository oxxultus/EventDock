package io.github.oxxultus.eventdock.storage.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MetadataCodecTest {
  @Test
  void roundTripsDelimitersAndUnicodeDeterministically() {
    var codec = new MetadataCodec();
    var metadata = Map.of("trace:id", "한글\nvalue", "empty", "");

    assertEquals(metadata, codec.decode(codec.encode(metadata)));
    assertEquals(codec.encode(metadata), codec.encode(metadata));
  }
}
