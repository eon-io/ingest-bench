# Flink configuration for the external-engine walkthrough

This directory contains the SQL job, submission settings, and cluster sizing
needed for Flink to consume a staged run. Follow the
[walkthrough](../../adding-an-engine.md) to replace the `@PLACEHOLDER@` tokens
and start the job.

The example uses raw Avro and is rendered for the local stack. A unit test checks
it against the managed Flink renderer. To build a managed integration, use
[`engines/flink/`](../../../engines/flink/) instead.

For another engine, provide the same three elements: a source with every corpus
column, the catalog named by the harness and an append-only insert. Column types
come from `corpus.json`; connection details come from `facts.json`.

| Token | Value from `facts.json` |
|---|---|
| `@RUN_ID@` | `run_id`, also used as the consumer group and job name |
| `@TOPIC@` | `topic` |
| `@NAMESPACE@` / `@TABLE@` | the two parts of `table`, split on the dot |

The catalog block includes the local stack's public MinIO credentials. Staging
redacts literal credentials from `facts.json`. For another environment, supply
credentials separately using the [site's environment references](../../run-spec.md#secrets).
