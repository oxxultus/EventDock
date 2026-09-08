package io.github.oxxultus.eventdock.autoconfigure;

import io.github.oxxultus.eventdock.core.UnitOfWork;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.transaction.support.TransactionTemplate;

public final class SpringTransactionUnitOfWork implements UnitOfWork {
  private final TransactionTemplate transactions;

  public SpringTransactionUnitOfWork(TransactionTemplate transactions) {
    this.transactions = Objects.requireNonNull(transactions);
  }

  @Override
  public <T> T execute(Supplier<T> action) {
    return transactions.execute(status -> Objects.requireNonNull(action).get());
  }
}
