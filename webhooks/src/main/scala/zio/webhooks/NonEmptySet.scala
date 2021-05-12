package zio.webhooks

final case class NonEmptySet[A](head: A, others: Set[A])
