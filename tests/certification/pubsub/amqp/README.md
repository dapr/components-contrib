# AMQP 1.0 pub/sub certification testing

The certification test for the `pubsub.amqp` component. It runs against Apache
ActiveMQ Artemis, which containerises cheaply and speaks plain AMQP 1.0.

## Test plan

### Delivery

- Publish messages through one sidecar and verify that two independent
  subscribers each receive all of them.
- Fail one message in every 100 from the first subscriber, to confirm that a
  message the handler rejects is delivered again.

  This case found a real defect. The component answered a handler error with
  the AMQP `rejected` outcome, which tells the broker the message is invalid,
  so it was dead-lettered and never came back. A handler error in Dapr means
  "deliver this again". The component now answers with `modified` and
  `delivery-failed`, which asks for redelivery and increments the broker's
  delivery count, so redelivery-delay and max-delivery-attempts apply and a
  message that always fails eventually dead-letters instead of spinning.

  The conformance suite does return a handler error, but it removes the message
  from its expected set before doing so, so it never asserts redelivery and
  could not detect the outcome being wrong.

### Recovery from a broker restart

Stop the broker container under a live subscription, start it again, and verify
that publishing and subscribing both resume with no sidecar restart.

This is the regression guard for reconnect. The component holds one connection
opened at `Init`. Before reconnect existed, a detached link made `Receive`
return the same error immediately and forever, so the subscription never
recovered.

### Recovery from a network interruption

Drop the broker port with no clean shutdown, then verify that delivery resumes.
This covers the path where the peer never sends a detach frame.

### Addressing with prefixes

Run a component configured with `topicAddressPrefix` and `queueAddressPrefix`,
and verify that publisher and subscriber still meet on the prefixed address.

What this proves is that the component applies the configured prefix
consistently on both the publish and the subscribe path, against a real broker.
Artemis auto-creates an address under whatever literal name it is given, so the
round trip succeeds without the broker being configured for those prefixes.

What it does **not** prove is routing-type separation. The broker here runs a
default acceptor, so it is not configured with `anycastPrefix` and
`multicastPrefix`, and nothing here checks that `queue:` reaches an ANYCAST
address while a bare topic reaches a MULTICAST one. See the known gaps.

## Running the tests

Start the broker and run the suite:

```bash
cd tests/certification/pubsub/amqp
go test -v -timeout 15m .
```

The suite manages the broker container itself through `docker-compose.yml`.

## Requirements

The network interruption step programs the host packet filter, so it needs
root. On macOS it fails with `Could not enable firewall using: sudo pfctl -E`.
It runs unattended on the Linux CI runner, which is where the mqtt3 and
rabbitmq suites use the same helper.

## Known gaps

- Routing-type separation is not covered. Proving it needs a broker whose
  acceptor is configured with `anycastPrefix` and `multicastPrefix`, which
  means a custom `broker.xml` mounted into the container.
- Message TTL is declared by the component but is not covered here or by the
  conformance suite.
- Wildcard subscriptions are not covered. The component does not declare
  `SUBSCRIBE_WILDCARDS`, because the syntax differs between brokers.
