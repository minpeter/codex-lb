## ADDED Requirements

### Requirement: Exact ownership of local bypass rules
The managed docker-lan-routes unit SHALL preserve the baseline destinations
172.17.0.0/16, 172.18.0.0/16, 172.19.0.0/16, 172.28.0.0/16 and 10.10.10.0/24.
It SHALL own only priority 5199 rules for those destinations in table main.
Start SHALL leave exactly one owned rule per destination; stop SHALL remove only
owned rules. Both actions SHALL be idempotent and propagate command failures.

#### Scenario: Another rule shares the replica destination
- **WHEN** start is repeated and then stop is repeated
- **THEN** each owned rule exists once after start and zero times after stop
- **AND** rules with another priority or table remain unchanged

### Requirement: Read-only replica route preflight
The preflight SHALL inspect the replica container network attachment, Docker
network IPAM, loaded service commands and status, IPv4 rules and route lookup.
It SHALL emit structured JSON and exit nonzero for subnet drift, missing or
duplicate owned rules, wrong selected bridge route, stale unit commands or
unexpected command failure. It SHALL NOT mutate routing or discover-and-apply
exceptions for arbitrary Docker or private subnets.

#### Scenario: Docker assigns a different replica subnet
- **WHEN** the replica attachment and IPAM no longer match 172.28.0.0/16
- **THEN** preflight exits nonzero with a subnet_drift finding without adding rules

#### Scenario: Exit-node default wins before recovery
- **WHEN** the managed unit starts in an isolated network namespace with a table 52 default
- **THEN** the replica route selects the local bridge instead of table 52
