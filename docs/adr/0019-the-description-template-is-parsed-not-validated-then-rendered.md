# 19. The description template is parsed, not validated then rendered

Status: Accepted
Nicotine+: No counterpart

## Context

A template that can fail to render must not reach the peer-response hot path.

## Decision

`RuntimeConfig.description` is a `DescriptionTemplate`, not a `String`. `DescriptionTemplate::parse` is the only validation: a consumer parses where it builds a runtime config, so a config that exists holds a renderable template. Rendering is one exhaustive match over two private variable enums, so adding a variable is a compile error until it is wired to a context field. `description_variables` lists the names for a consumer to document.

## Consequences

A `validate()` beside a `render()` would be two copies of the grammar plus unreachable error arms. newkitine ADR 0032 records where newkitine parses it.
