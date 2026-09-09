## ADDED Requirements

### Requirement: Authorization follows current credentials and raw peer trust
Sessions authenticated with replaced credentials SHALL NOT regain authority
after removal and rebootstrap. Authentication management endpoints SHALL enforce
the same binding. Trusted-header authorization and header sanitization SHALL
use the captured socket peer rather than the projected end-user address.

#### Scenario: Replay old administrator cookie after rebootstrap
- **WHEN** an administrator removes and replaces the dashboard credentials
- **THEN** a retained old cookie cannot authorize dashboard or credential changes

### Requirement: Unrelated edits preserve restrictive account scopes
Editing an orphaned scoped API key or automation SHALL preserve its empty
restricted account set unless the operator explicitly broadens it. OAuth manual
callback completion SHALL only update the flow generation that initiated it.

#### Scenario: Rename orphaned scoped key
- **WHEN** the key has enabled account scoping with no surviving assignments
- **THEN** a name-only edit does not grant access to the entire account pool
