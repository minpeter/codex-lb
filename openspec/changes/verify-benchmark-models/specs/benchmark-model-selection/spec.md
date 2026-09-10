## ADDED Requirements

### Requirement: Explicit benchmark model selection
The benchmark SHALL accept an optional nonempty JSON list of unique, nonempty
string model names without whitespace. It SHALL validate keys and reasoning
effort only for selected models, ignore unselected map entries, and default
omitted effort to low. Omitted models SHALL retain Astra, Luna and Terra defaults.

#### Scenario: Multiple custom models
- **WHEN** a protected config selects custom-low and custom-minimal with keys
  only for those models and minimal effort for custom-minimal
- **THEN** scheduling, request payloads and summaries contain exactly those models
- **AND** custom-low uses low effort and custom-minimal uses minimal effort
- **AND** each model has adjacent opposite-arm pairs with exact AB/BA balance

#### Scenario: Invalid names have matching keys
- **WHEN** the list contains duplicate, empty or whitespace-containing names
  even though matching nonempty keys are supplied
- **THEN** configuration loading rejects the model selection before any request

#### Scenario: Default compatibility
- **WHEN** models is omitted
- **THEN** configuration, scheduling and summary membership retain the existing
  gpt-6-astra, gpt-5.6-luna and gpt-5.6-terra defaults

### Requirement: Local verification and artifact isolation
Generated benchmark artifacts SHALL be excluded from Git and Docker contexts.
Completed campaign journal rows SHALL equal ordered samples plus the seed.

#### Scenario: Synthetic Sol campaign
- **WHEN** the real CLI runs ten rounds against a loopback Responses server
  with only gpt-5.6-sol selected
- **THEN** it sends twenty requests, ten with priority and ten with tier omitted
- **AND** configured client headers reach both arms and the journal matches samples
- **AND** output contains neither credential values nor streamed text

#### Scenario: Historical artifacts remain private
- **WHEN** scripts/qa/artifacts contains historical results or failures
- **THEN** normal Git staging and Docker context packaging exclude that directory
- **AND** benchmark source outside that directory remains eligible for packaging
