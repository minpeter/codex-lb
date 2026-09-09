## ADDED Requirements

### Requirement: Maintenance and synchronization preserve persistence identity
Remote synchronization SHALL skip pending-delete rows without duplicate-key
insertion. SQLite maintenance SHALL use the same filesystem path as the runtime
engine, including literal hash characters. Backup rotation SHALL retain the
newest backup when timestamps collide and return an existing recovery file.

#### Scenario: Two backups within the same second
- **WHEN** retention permits one backup and a second backup has newer content
- **THEN** rotation retains the second backup and removes the older content
