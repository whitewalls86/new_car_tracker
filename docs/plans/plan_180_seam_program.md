# Plan 180: the seam program

## What this plan is for

Gives every boundary in the system — service-to-service HTTP, the database,
the object store, the metrics stack — one declared way across it, with rules
that make silent drift fail loudly and ledgers that measure the conversion
until every ad-hoc path is gone.

## The case

**Plan 162 built the seam-closing machinery one instance at a time, and the
thirty-first stage was the one that finally named the template.** Stage W
closed the database-vocabulary seam; Stage AA and AL closed the HTTP seam to
grade 2 — comparators and shrink-only ledgers holding a measured gap of some
three hundred entries. Along the way the repository wrote the same statement
four times ("a test double derives from the owner artifact of the seam it
stands on" — `test_no_mock_invents_a_shape.py` counts them itself) because
the general form had no home, and grew a contract document of 29,000 words
whose table rows restate what rule docstrings and plan records already say.

**The census sorted all 83 asserted-rule rows into seams and graded them**
([`seam_rule_census.md`](../planning/seam_rule_census.md)): four seams are
already at the ideal — SQL is the existence proof that heavy standardization
leaves only cheap border guards — two need one upgrade each, the HTTP and
mocking seams sit at grade 2 with heuristic readers holding the unconverted
majority, object storage has one governed artifact and an unowned channel,
and metrics sits at grade 0 despite having already cost eight silent hours
of a stale-gauge outage nothing alerted on.

**The case for the program over the status quo is the cost curve of the
heuristics.** A grade-2 seam's readers must recognise every ad-hoc shape,
which is why the HTTP seam alone carries hundreds of lines of AST analysis
that a declaration helper, a generated route registry and a real-response
double builder would delete — the same trade Stage X already made once,
moving 505 SQL literals so a judgement rule could be struck. Comparing
forever means maintaining that analysis forever; converting means each
reader retires the day its ledger empties, having verified every conversion
on the way out. The ideal-state specification
([`seam_ideal_state_spec.md`](../planning/seam_ideal_state_spec.md)) names
the mechanisms, the border guards that survive, the retirement map, and the
one new meta-rule that makes retirement forced rather than remembered.

**Origin, recorded:** split out of Plan 162 on 2026-09-10, from the Stage AL
session that closed the HTTP seam's rule surface and then, asked whether the
new rules made old ones redundant, found the question unanswerable without
the seam frame. It supersedes Stage AL's conversion tail, absorbs Stage AM's
exit shape, reshapes Stage AH (skills written against the ideal corpus
rather than the interim one — three interim drafts were paused for exactly
this reason), and is the intended home for Stages AD, AE and AJ pending a
check of each against the frame. The `docs/TESTING.md` restructure follows
the same spine — one section per seam — and lands here rather than in a
separate effort, because the census is what makes it mechanical.
