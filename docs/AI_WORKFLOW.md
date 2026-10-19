# AI workflow

The full Raliess application uses an OpenAI-compatible model interface with lighter and stronger roles. A local model or configured hosted provider can fill those roles. The current installation uses a local quantized Gemma 4 model with cloud access disabled.

## Stages

| Stage | Model task | Output |
| --- | --- | --- |
| Product profiling | Extract only supplied product facts from a website or operator brief | Structured summary, value propositions, proof points |
| Segment planning | Propose likely customers and buyer roles | Segment names, fit scores, angles, titles, search queries |
| Prospect research | Find one specific observation supported by the company's own material | One sentence or an empty result |
| Cold-email writing | Combine the brief, recipient context, evidence, sender voice, and recent drafts | Structured subject and body |
| Reply handling | Classify inbound intent and use the full thread to answer in the sender's voice | Intent, confidence, and a reply draft |

## Draft controls

The writer receives approved facts and forbidden claims from the product brief. It sees recent subjects and openings to reduce repetition. After generation, deterministic checks examine length, banned phrases, duplicate subjects, similar openings, sign-offs, and brief guardrails. A rejected draft can be regenerated with specific feedback. Sending is a separate step.

Research can return empty when a company's site has no usable detail. That is safer than inventing a personalized fact, but it also makes the draft less specific. Internal evaluation found that missing site text and unsupported claims remain important quality limits; prompt checks do not solve them automatically.

The actual prompt bodies and campaign orchestration code are private. This document describes the interfaces and controls used by the system.
