# Verification ledger

All fifteen repair criteria have current source/regression coverage.
Parent final backend run:1707passed196.25seconds; fullty, unchangedarchitecture
ratchets, strictOpenSpec and gitdiffcheck passed. Earlier partialruns not
counted as fullrepositoryproof. Frontend52focused+productionbuild passed;
worker fullfrontend1242tests passed. ActualChromium12scenarios inspected
requestPATCHes: unchangedorphanscope omitted, explicitclear[], rescopeIDs.

Actual isolatedcandidateHTTP exposed a legacy authfallthrough after initially
greenASGItests. Addedprotectedroute assertions yielded2REDfailures; finalguard
now rejects oldcookie401 afterpasswordchange/rebootstrap, freshlogin/access200.
TOTP reenrollment tests use fixedclock. Credentialchangeinvalidates oldcookies;
operators must login again after upgrade. No productioncredentials mutated.

WSactualsender/reader mutationRED provedlateAclaimB beforefix; sourceowner
andsetupcancel behavior tests use realASGIand cleanup events. ParentretryRED
health-before-release fixed by waiting forsettlement; clampmutationRED and
37relatedtests passed. BridgeREDglobalaliasloss fixed, cached/inflight/hardowner
threecases passed and sharedaliases preserved. DB/remote/drainREDfixtures
were verified in focused and finalintegratedruns.

Evidence: /home/minpeter/github.com/minpeter-labs/.omo/evidence/
mass-repair-parent-verification.md, mass-logic-candidate-http.json,
mass-logic-browser-qa/receipt.json andnetwork-payloads.json. WS artifacts
/tmp/codex-lb-ws1-st_01a087ce/. LSPdaemon timedout; CLItypassed.

QAcontainers andbrowser closed, synthetic DBs only. Bonlydeployment and
finalultrabrainaudit pending. Preexistingbenchmarkchanges excluded.
