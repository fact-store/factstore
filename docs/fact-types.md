# Fact Types - Why and How?

In order to ensure data integrity of the fact store, stored facts should conform to specific data model or schema.

## What is a fact type?

- A fact type is an object that describes a specific kind of fact in more detail.
- A fact type doesn't contain any specific fact, i.e. it is not a fact.
- A fact type must be registered with the fact store.

## Fact Type Specification

A fact type object must contain the following information:

- `group`: the group of events this type belongs to (example: `io.factstore`)
- `name`: the name of the type (example: `task-added`)
- `version`: the version of the type (example: `v1.0`)

The fully qualified fact type can be composed of these attributes: `io.factstore.task-added:v1.0`

## Open Questions
