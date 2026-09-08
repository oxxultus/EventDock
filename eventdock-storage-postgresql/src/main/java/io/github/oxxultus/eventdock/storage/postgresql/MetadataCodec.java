package io.github.oxxultus.eventdock.storage.postgresql;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

final class MetadataCodec {
  private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

  String encode(Map<String, String> metadata) {
    return metadata.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> encodePart(entry.getKey()) + ":" + encodePart(entry.getValue()))
        .reduce((left, right) -> left + "\n" + right)
        .orElse("");
  }

  Map<String, String> decode(String encoded) {
    if (encoded == null || encoded.isEmpty()) {
      return Map.of();
    }
    Map<String, String> metadata = new LinkedHashMap<>();
    for (String line : encoded.split("\\n", -1)) {
      int separator = line.indexOf(':');
      if (separator < 0) {
        throw new PostgresqlStorageException("invalid encoded event metadata");
      }
      metadata.put(decodePart(line.substring(0, separator)), decodePart(line.substring(separator + 1)));
    }
    return Map.copyOf(metadata);
  }

  private String encodePart(String value) {
    return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private String decodePart(String value) {
    return new String(DECODER.decode(value), StandardCharsets.UTF_8);
  }
}
