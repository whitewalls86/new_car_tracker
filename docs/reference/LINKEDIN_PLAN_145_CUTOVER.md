# LinkedIn Article D — The April Cutover

Draft, 2026-09-01. Source:
[`docs/evidence/plan_145_post_mortem_draft.md`](../evidence/plan_145_post_mortem_draft.md).
Slot D in the corpus table in [Plan
138](../plans/plan_138_public_surface_refresh.md).

**Spine:** chronological — the four designs in order — but the payoff is the
parser control, not the deletion. The article argues that bronze earned its keep,
and that the over-defensive machine is what proved it.
**Companion post:** style contagion, standalone at ~300 words.

**Register filter applied** per Plan 138's Gate for D: no internal hostnames, no
production object keys or prefixes, no run identifiers, no approval records, no
incident payloads, nothing about the scrape path. Every number below appears in
the public repository or the public weekly recap.

---

## ARTICLE (Long-form LinkedIn article)

**Title:** I Set Out to Delete 13 GiB of Old HTML. The Payoff Was Data I Didn't Know I Had.

---

Here is the job, stated as plainly as I can.

I had 1,172 files holding raw HTML — saved car listing pages from April, four
months earlier. 13.66 GiB on a single VPS that did not have 13.66 GiB to spare.
They were the last remnant of moving this project off hardware I owned and onto a
cloud VM, and everything in them had, in theory, already been re-stored somewhere
better.

The obvious move is one line: a prefix and a delete.

"In theory" is why I could not. Checking it meant reconciling **four different
records of the same April, no two of which agreed.**

**The legacy Parquet** held 951,821 rows of raw HTML plus the old system's
metadata — and 43,014 of those rows carried a real, recorded hash for a body that
was **empty**, 39,988 of them logged as successful fetches.

**The current storage layer** held 557,065 April pages, each with a small sidecar
of identity — where the listing field was wrong more often than it was right, and
blank on 99,981 pages.

**The processing queue** knew which pages had been ingested and when, except that
it had **no row at all** for 42,276 of those 557,065.

**The parsed output** was what the pipeline had actually understood. It was
missing for 99,981 pages, and everything before the cutover came from a schema
with seven fewer columns.

Read those four back and notice there is nothing you can stand on.

Now the shape of the actual problem, which is simpler to state and worse than it
sounds. There were three populations, and I knew the size of each one:

- **Bronze** — raw detail pages held in the current store: **557,065 pages**
- **The Parquet** — distinct detail captures with real bytes, out of 951,821
  rows: **797,073 pages**
- **Silver** — April observations produced *by* detail pages: **1,267,812 rows**

**What I did not know was how those three sets overlap — and read the units
again, because they are not the same units.**

Two of those count pages. The third counts rows, and one page does not make one
row. A detail page yields one observation about the car it is actually about,
plus five or six more about the other cars in the carousel beside it — between
six and seven observations from a single fetch, and for April a further **5.95
million carousel rows** riding on the same pages. So you cannot intersect a page
count against a row count without a trustworthy page-to-listing mapping, and that
mapping is precisely what was broken. I did not know that yet.

The rest of the overlap was just as open. Every stored page should have produced
at least one parsed observation; for 99,981 of them, none exists. Some of the
Parquet is certainly a second copy of pages already in the store — *which ones,
and how many,* was the question. And **690,166 of those 1,267,812 detail
observations — 54% — sit on the legacy side of the 21 April seam**, described by
a schema missing seven columns the current one has.

So the question was never "can I delete these 1,172 files." It was: *prove that
every body inside them exists somewhere else, without trusting a single thing any
of those four systems recorded about any of it.* The only identifier that
survived scrutiny was the hash of the bytes themselves.

And consider what this data actually is. A snapshot of a web page, one day fresh
when captured and four months old by the time I looked at it. Nobody was going to
read it. Nobody was waiting on it. The listings it described had mostly sold.

So: forensic accounting, to the individual byte, on garbage.

What I built was a system that hashed all 983,043 pages, copied every one of them
into a second location, hashed them again, deleted the copies it could prove were
duplicates, re-packed the survivors, extracted every single one back out of the
archive it had just written to confirm the bytes had actually landed that way,
and only then — after a coverage check that refused to remove any file whose
contents were not provably present somewhere else — deleted the originals. Three
days. About thirty hours of compute. 9,410 lines of Python, 7,107 lines of tests,
seventeen subcommands, and four separate designs — the first of which I merged,
deployed, and reverted the same evening.

To save four-month-old HTML.

Step back far enough and that is genuinely hard to defend. I want to walk it in
order anyway, because at the end a check I had built to prove nothing had gone
wrong went off — and what it found was worth more than the 13 GiB.

---

### Where this came from

None of this was anybody's mistake. It is the residue of a month in which
everything went right.

- **April 8th** — An archival service ships. It compresses raw HTML into Parquet
  before the local disk is cleared. **These are the 1,172 files.**
- **April 14th** — The project moves off hardware I owned and onto a cloud VM.
  Fresh Postgres, new tables, sequences starting at one.
- **April 21st** — The new processing service goes live. **This is the cutover**,
  the seam that runs through April.
- **April 27th** — 13.7 million legacy observations are migrated onto the new
  storage. Their artifact IDs are remapped into a range that cannot collide with
  the new sequence.
- **April 28th** — The analytics layer is rebuilt on the new storage and the
  migration is declared done.

Every one of those succeeded. The migration on the 27th even solved the exact
identity collision that runs through everything below, remapping the
identifiers so nothing could collide. It just did it for the *observations*.

The archival service's Parquet was not an observation, and was in nobody's scope
on any of those five days. It kept writing straight across the move — its files
span the 11th to the 21st, stopping dead at the cutover — and it kept the **old
numbering** throughout. Two systems, each minting IDs from its own counter
starting at one, so the same integer names two different pages on opposite sides
of the move. A join on it returns plausible, confident, wrong answers, which is
worse than returning nothing.

That is the shape of most migration debt: not a mistake, an omission — invisible
until something later needs to join across the seam. And the seam was not the
only thing left unowned.

**The storage layer was holding 42,276 pages that nothing else had ever heard
of** — no ingestion record, no parsed row. That is not an accident either. The
service that compacts old pages works from a listing of *what is physically in
storage*, not from a list of what the system knows about; its own comment says an
object nobody can describe is still an inode, and leaving it out would leave it
unpackable forever. So it dutifully archived all 42,276 while recording, in their
metadata, that it had no idea what they were.

Without opening them I could not tell whether they had been processed, whether
they duplicated something in the Parquet, or whether they were junk. When I did
look: **36,241 of the 42,276 were full-size, intact pages** — real captures,
byte-verified, that no other part of the system knew existed. The junk was in a
different pile: a further 57,705 pages *did* have an ingestion record and no
parsed row, and 48,600 of those are 441-byte stubs with nothing to recover.
Between the two piles, 99,981 pages had no usable identity.

**That ignorance had already sent me a bill.** When April was first compacted,
the 99,981 pages with no identity sorted to the end and landed in six archives of
their own. The compactor groups pages by listing so that similar pages compress
against each other; with no listing to group by, it saw one undifferentiated blob
44,000 members long and never found a boundary. The full-size ones among them
cost **7,157 bytes each against 3,900** for a page the system could name — about
143 MB of pure penalty, and the reason April compacted to 53.4% when I had
projected 74%. Not knowing what your data is has a storage price, and it is
payable in advance.

That measurement is also what surfaced the orphans in the first place. I filed
the investigation the same night, and a week later folded it together with the
legacy-Parquet question into a single plan. So none of this is background to the
story — these are the threads that *became* it.

Which is the lesson, and it arrives before a single line of code is written:
**a migration is finished when the old thing is deleted, not when the new thing
works.** Everything below is what it costs to finish one late.

---

### Design one: take the shortcut, and ship it

The first design was the sensible one, and it is worth noticing that it was
essentially the shortcut with arithmetic behind it.

A read-only census said something encouraging: of 355,845 captures with no
matching record in the current system, only **270** ever witnessed a price
change. Everything else was a duplicate observation of a price already recorded.
So: recover the ~11,600 information-bearing rows, drop the rest, delete the
files, go home.

Hold on to the assumption underneath that, because the whole project ends up
testing it: *the boring rows are safe to lose.*

This is the design that shipped. It was built, merged, pulled onto the server,
rebuilt into two containers, and health-gated into production. And then it could
not start.

The script needed to know where the legacy files lived — the storage prefix. That
prefix was not in a config file, not in a constant, not in the plan document. It
existed in exactly one place: a descriptive sentence in an older planning
document, written in prose, for a human to read. We had closed six preparation
gates before a line of code was written, and not one of them was "name the
input."

The model I was building with had flagged this hours before the deploy:

> I do **not** have the literal […] prefix confirmed anywhere in the docs or code
> — [the older plan] only describes it in prose […]. Don't let me guess that
> string; list it first.

*(Trimmed only to remove a production storage path; the sense is unchanged.)*

Then, one message later, it guessed anyway. The guess happened to be right. The
comment that eventually went into the shipped code explains why that was still
the wrong move: **a hardcoded guess that matches nothing does not throw an
error — it returns an empty population, which looks exactly like success.**

It was reverted four hours later, and the reason is not complicated: it did not
work, and it had made itself expensive to leave lying around. To carry a
historical backfill it had restructured **two live production write paths** — 471
changed lines in one, 69 in the other — all to serve a backfill that had just
proved it could not run.

One thing in it did survive, and I want to flag it now rather than take credit
for it later. The ledger this design built joined the old store to the new one
**on the hash of the page contents, never on an identifier** — it says so in the
docstring. The right instinct was in the first file I merged. What took another
three designs was working out that having the right key is worth nothing while
the work still has two stores to reconcile.

---

### Design two: join the old store to the new one

Fifty-two minutes after the revert, the plan was rewritten, and this is what
replaced it.

Each file in the new store carries a small sidecar of metadata, including which
listing the page belongs to. That looked like a bridge: match old to new on that
identifier plus a timestamp, and you have a ledger of what has been recovered and
what has not. It is the obvious thing to reach for, and it is the one idea in
this project that never got as far as code.

What turned up the next morning killed it.

The sidecar identifier — the join key the entire design rested on — is **wrong
for 194,639 of 371,095 content matches**. Right timestamp, wrong listing. And
when the two sources disagreed, the *older* store was right: sampling the
disagreements, the legacy identifier was corroborated by downstream data in
194,734 of 194,734 cases. Not most. All of them.

---

### Design three: refuse all metadata

So design three refused metadata entirely. Don't join on anything anyone derived;
take the union of both stores, deduplicate by content hash, re-parse everything
from raw bytes.

The instinct was right and it survived into what finally shipped. The design did
not. It carried a two-store reconciliation through every single stage, so every
count, every gate and every report had to explain itself twice — and it was
costed at **an estimated 24.8 core-hours**, which is the number I stopped on. I
should be honest that this is the weakest of the three verdicts: design three was
never run, so what killed it is a projection, and the design that replaced it
went on to spend nearly seventeen hours parsing the same population anyway.

Three designs were discarded and no two died of the same thing: the first of an
**unnamed input**, the second of **identity**, the third of **cost**. Only one of
those three is a fact about the data. But identity is the through-line — it is
what the second design was reaching for and what the first could not have
survived contact with — and the design that shipped won not by solving it but by
arranging the work so nothing had to be joined at all.

**Bytes and hashes never lied. Every derived key did.** In a population where
the sidecar was wrong 52% of the time, a recorded hash could describe an empty
file, and 42,276 pages had no record at all, that was not a stylistic
preference. It was the only ground left.

---

### One line of SQL, and 3.6 million unreliable objects

I owe you the mechanism, because "the sidecar was wrong 52% of the time" is
doing a lot of quiet work in that last section.

A detail page is a page about one car. It also shows you five or six other cars —
a carousel of similar listings down the side. The pipeline reads all of them,
because that data is free and useful, and writes one row for the page's subject
and roughly 5.7 more for the others. **All of those rows share one artifact ID**,
because they all came from one fetched page. That is correct and intended.

Months after those pages were captured, the compaction service needed to label
each stored one with the listing it belonged to, so that pages about the same
car would sit next to each other and compress well. It asked the obvious
question — *for this artifact, which listing?* — in the obvious way:

> `any_value(listing_id) GROUP BY artifact_id` — and no filter on source.

There are ~6.7 listings behind each artifact ID and exactly one of them is the
page's subject. `any_value` returns whichever it happens to see first.

> **⟨ FIGURE 1 ⟩** *Identity correct by capture month: April
> 31.4% of 557,065 objects · May 59.5% of 1,021,266 · June 9.8% of 1,124,122 ·
> July 8.4% of 909,654 · all four, 26.8% of 3,612,107.*
> **Caption:** Same code, same query, same defect — and the error rate swings
> 40% to 92%.

**Every one of those 3.6 million objects had its identity written by that line,
and about 2.6 million of them are wrong.**

The month-to-month spread is the strangest part. Same code, same bug, and the
error rate swings from 40% in May to 92% in June. `any_value`
picks by scan order, and scan order shifts with data volume and layout. The
accuracy of that field is a function of how the query planner felt that month.

It lived fourteen days, and in that fortnight it labelled every object in four
months of archive. Two things let it survive even that long.

**Nothing reads it.** Serving a stored page looks it up by object key and
verifies the content hash. The integrity checker validates member counts, frame
ordinals and byte offsets. No read path consults that field, so the defect has
never served a single wrong byte to anybody. It is invisible from every direction
except the one I approached from.

**The tests could not catch it.** Every test fixture gave one silver row per
artifact — so `any_value` had exactly one value to choose from and could not
pick wrong. The fixture was built to make the happy path convenient, and in doing
so it removed the only condition under which the bug exists. (The regression test
written afterwards now emits the six carousel rows *before* the subject
deliberately, so a reducer that ignores `source` cannot pass by luck of scan
order.)

And then the fix could not be the obvious one, which is my favourite detail in
this whole project.

That same wrong field is the packer's **sort key**. Pages are ordered by it, and
each compressed frame is sealed at a boundary between listings. Adding `WHERE
source = 'detail'` would have corrected the label and silently re-laid-out every
pack in the archive — millions of objects rewritten to fix a field nothing reads.

**The wrong value was load-bearing for the physical layout.**

The eventual fix splits the two jobs apart: one field for *placement*, frozen as
it is so no byte moves, and one for *identity*, recomputed correctly. The
sidecars were corrected without relocating a single member.

---

### Design four: the one I came up with, out loud

By this point I had shipped one design and watched two more go by, each better
than the last and each more elaborate than the last, and I said something less
articulate than any of them:

> I want to understand why this has been so complicated. I feel like we've taken
> eight stabs at this, each of them better, but each of them convoluted, when
> what we're really trying to do is pretty simple.

The design that came out of that is one sentence: **flatten first.** Copy every
surviving page out of both stores into one flat directory of ordinary compressed
objects. Delete the duplicates by content hash. Unpack the archives. Now every
stage downstream reads *one* store, and the two-store reconciliation that had
been threaded through everything simply evaporates.

Eight minutes later I said the other obvious thing — do the duplicate deletion
*before* the unpack, so you never pay disk for copies you are about to throw
away. Both came from a human in plain language, in one sitting, and both survived
to production unchanged.

**And that is how the intersection finally got computed: by brute force, because
no identifier could do it.** Once every surviving body was one object in one
place, the content hashes answered the question directly.

> **⟨ FIGURE 2 ⟩** *557,065 pages already in the store, plus
> 425,978 held only in the legacy Parquet, equals a union of 983,043. Of the
> store's 557,065, exactly 371,095 were byte-identical twins of Parquet pages.*
> **Caption:** The intersection no identifier could compute.

Of the 797,073 distinct captures in the Parquet, **371,095 turned out to be
byte-identical twins** of pages already in the store and **425,978 existed
nowhere else**. Added to the 557,065 already held, that is a union of
**983,043** — the population every later stage reads.

Every design before this one had been an attempt to derive those four numbers
from metadata. It took 6h26m of copying, hashing and unpacking to simply measure
them.

I do not think the model was being obtuse. It was doing what a strong engineer
does when handed a constraint: making the constrained thing better. It took
someone standing outside the constraint to ask whether the constraint had to be
there.

---

### What "defensive" actually meant here

Nothing in that job needed to be careful. Here is how careful it was anyway.

Objects were keyed by the hash of their own contents, so a crashed run resumed
instead of duplicating. All 807,797 copied bodies were read back and
hash-checked — zero failures, a boring result that took hours to earn. Every one
of the 983,043 loose copies had to clear three predicates before deletion: the
key resolves to the right archive, the member extracted from that archive
matches its recorded hash, and the loose copy matches the packed bytes exactly.
The final deletion went by name, from a manifest written before the first
delete, in capped batches, never by prefix.

Two of those deserve a sentence each, because they are the ones I would not have
thought of unprompted. The verification **refused the convenient path**: each
member was extracted from the specific archive its own sidecar named, rather than
through the normal read path, because the old sidecars would still have answered
correctly for 557,065 members while both sets existed — the easy check would have
passed for the wrong reason. And the write **happened twice on purpose**: five
hundred rows went first as a canary, with six protected tables proven
byte-identical before and after, and the full write then excluded exactly those
rows so the arithmetic closed in both directions.

Set end to end, that is an absurd amount of apparatus for four-month-old HTML.
Except for one finding, early on, that I have thought about more than anything
else here.

**43,014 of the legacy rows had a real, recorded sha256 for a body that was
empty.** Not corrupt — empty. The original writer had archived zero bytes when
the page file was already gone, while still faithfully copying the content hash
out of the database. **39,988 of those were HTTP 200s.** By every piece of
metadata the system had recorded, those captures were fine.

Any design that trusted recorded hashes instead of recomputing them from the
actual bytes would have counted 43,014 successes it did not have. The distinct
*stored* hash count was 837,061; the distinct *recomputed* count was 797,073.
That 40,000-object gap is not a bug in the recovery. It is the recovery telling
you what the metadata was worth.

That is the moment the paranoia stopped being aesthetic.

---

### The run, and the lie a sample told

- **materialize** — 807,797 pages copied into one store — **4h10m**
- **dedupe** — 371,095 content duplicates deleted — **13 min**
- **unpack** — 557,065 archived members expanded — **2h03m**
- **parse** — 983,043 pages into 5,738,532 observation rows — **16h45m**

Those four stages are twenty-three hours of the three days; the repack, the
prune and the trial take the run to roughly thirty. The stages are strictly
sequential — nothing parses until it is unpacked, nothing unpacks until the
duplicates are gone — so there was no version of this that went much faster,
however well I had designed it on the first try.

While the sixteen-hour parse ran, I tested the write path against a partial
sample of its output. A check reported that **0 of 42,276** previously-archived
pages had gained an identity from the recovery — those 42,276 being the orphans
from the top of this article, the ones nothing in the system had a record of.

Zero is a clean, believable number. It was also structural: the parse processed
the unpacked shards last — 2.7% of the work units, 56% of the wall clock, all at
the end — so a mid-run sample could not physically contain one of them. The real
figure was **36,220 of those same 42,276 — 85.7%.** Six out of seven of the pages
nobody could account for now had an identity.

A sample that cannot contain the cohort you are checking does not return a small
number. It returns a confident zero.

---

### The check that failed

The comparison itself was undramatic, and worth stating because it is the answer
to the question the whole project asked. Of the **5,738,532** observation rows
the recovery parsed out of those 983,043 pages:

- already present in the parsed output — **4,977,697 rows, 86.74%**
- quarantined as error pages rather than observations — 59,460 rows, 1.04%
- **genuinely missing — 701,375 rows, 12.22%**
- unclassifiable — **0**

So the belief I started with was 87% right, and the remaining 12% is the reason
none of this could be settled by argument.

Then, before any of it was allowed near the real tables, there was a control:
parse a sample of pages the current system had *already* processed successfully,
and compare the recovered output field-by-field against what production wrote at
the time. If they match, the recovery parser is trustworthy. It is the check the
entire plan rested on.

It ran for the first time and **failed. 233 of 498 compared rows disagreed with
production — 46.8% — across 2,867 individual fields.**

That is the number that should end a project. The whole argument for importing
any of this was that reprocessing raw bytes reproduces what production already
believed, and it emphatically did not.

The diagnosis took under an hour, and it was not a parser bug.

Four months earlier, a migration had brought older observations into the current
tables from a **legacy schema missing seven dealer-side columns.** Everything
before the cutover date came from that schema; everything after came from the
current pipeline. April straddles the boundary, so the comparison was running
across two different definitions of "a row."

Split on the cutover date, over 19,872 rows compared at exact timestamp distance:

> **⟨ FIGURE 3 ⟩** *After the cutover: 11,665 rows compared, 4
> disagree (0.03%), mean 0.00 fields recovered. Before the cutover: 8,404 rows
> compared, 8,404 disagree (100.0%), mean 12.19 fields recovered.*
> **Caption:** The check failed on one side of the seam, and it failed upward.

Read those two lines twice, because it took me a moment.

**After the cutover, the recovery agrees with production 99.97% of the time** —
so the parser is sound and the plan's central assertion holds.

**Before the cutover, it disagrees 100% of the time — because it has more.** A
mean of **12.19 fields per row** that the original pipeline never captured and
the legacy schema had no column to hold. Not corrections. Additions. Data sitting
in those HTML bytes the whole time, that nobody could see, because in April the
pipeline reading them did not know how to look.

The check that failed is the one that paid.

My reaction in the moment was less considered than that:

> It'd be a shame to throw it away after we spent 16 hours creating it.

The parsed output was retained rather than discarded, and it now has a plan of
its own: an estimated **~2 million rows** across the affected window stand to
gain those fields. I want to be honest about the state of it — measured and
frozen, not applied. Writing row-level updates into the observation table safely
needs a storage change that has not landed yet, so the enrichment sits in the
backlog behind it. What is *proven* is the 12.19, on real rows, by measurement.
What is pending is spending it.

---

### And then, almost as an afterthought, the deletion

1,172 planned, 1,172 deleted, 1,172 reconciled. Zero refused, zero absent, zero
errors. Under a minute. April went from 24.48 GiB to 4.34 GiB — **20.14 GiB
reclaimed**, more than the 13.66 the job set out to remove, because the
flatten-everything design created an intermediate population the same pass then
cleaned up.

It was boring, which is the only acceptable adjective for an irreversible
operation on production data.

And it is the least interesting number in this article.

---

### What the 13 GiB actually bought

The reason to keep raw source data is usually stated as insurance: if the parser
has a bug, you can fix it and reprocess. That is true, and it is a weak argument,
because it asks you to spend real storage on a hypothetical.

This is the strong version, and I did not have it until this project produced it.

**Your parser gets better over time. Your stored, already-parsed results do
not.** The pipeline that read those pages in April was working correctly by its
own standards; it simply lived in a schema with seven fewer columns. Every
improvement made since — every field added, every extraction sharpened — applies
only to pages arriving *after* the improvement, unless you kept the original
bytes. Raw storage is not insurance against your mistakes. It is the only
mechanism that lets you spend today's understanding on yesterday's data.

Design one would have deleted those pages. It was the sensible design, backed by
a correct measurement, and its assumption — *the boring rows are safe to lose* —
was true about prices and false about everything else on the page. The 270 rows
that witnessed a price change were the only value anyone had thought to look for.

So was all that apparatus disproportionate? It was disproportionate *to the
deletion*. It turned out to be exactly proportionate to
what was in those files, which nobody knew, because nobody had looked — and the
only reason anybody could look is that the bytes were still there.

The 1,172 files are gone. The 12.19 fields per row are not.

---

## COMPANION POST (Short LinkedIn post linking to the article)

I asked for a one-off script. I got a fortress.

The job was to delete 1,172 files — 13.66 GiB of four-month-old HTML on a VPS
that didn't have it to spare. If you're brave, that's one line.

What I ended up with was 9,410 lines of Python, 7,107 lines of tests, seventeen
subcommands, and four separate designs — the first of which I merged, deployed,
and reverted the same evening.
It hashed every page, copied all of them somewhere else, hashed them again, and
refused to delete anything it couldn't prove existed in two places.

Not because the model I was building with was timid. Because my codebase is
defensive — manifests written before anything is deleted, receipts on every
write, gates that fail closed, nothing removed by prefix — and it read all of
that, matched it, and wrote a throwaway script in the style of production code
I'd already defended in review.

That's mostly a gift. It's the best argument I know for investing in your own
conventions, now that conventions are executable by something that reads them.

The catch is that a model has no concept of "this script runs once." It can't
tell you when your own standards don't apply, and it won't tell you it isn't
making that call. I spent three days sure the whole thing was disproportionate.

Then the last check before deletion failed — 46.8% of the sampled rows
disagreeing with what production had recorded — and the diagnosis was that the
recovery wasn't wrong. It had **more**. A mean of 12.19 fields per row that the
original pipeline never captured, because in April it was reading those same
bytes through a schema with seven fewer columns.

It was disproportionate to the deletion. It was exactly proportionate to what was
in the files, which nobody knew, because nobody had looked.

Full write-up: [link]

Anyone else had a defensive check pay out as a discovery rather than a save?
