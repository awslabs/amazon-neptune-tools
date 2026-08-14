# neptune-schema-stats

A read-only CLI utility for Amazon Neptune that combines the Graph Summary
and Schema APIs to produce per-label structural statistics — node/edge
counts, property fill rates, characteristic-set analysis, and multi-label
partitioning — for both property graph (PG) and RDF Neptune databases.

## Why it exists

Neptune's Graph Summary API gives you graph-wide totals and characteristic
sets, but doesn't answer "how many nodes carry label X?" or "how does that
break down across labels sharing property signatures?". The PG Schema API
adds label-level context but still leaves ambiguity when multiple labels
match the same characteristic set.

This tool correlates the two sources, resolves any remaining ambiguity with
targeted `count(*)` queries, and reports each label's exact count, property
population, and edge cardinality. Everything runs against metadata APIs
plus a handful of scoped queries — no full-graph traversal — so it works
on clusters of any size in seconds.

## Dependencies

- Python 3.13 or newer (Python 3.14 works)
- Network access to your Neptune cluster (typically via VPC / bastion /
  port forward)
- IAM permissions:
  - `neptune-db:GetGraphSummary` (always required)
  - `neptune-db:ReadDataViaQuery` (required for schema fetch, multi-label
    probe, class-count probe, and scan queries)
- **Neptune engine 1.4.8.0 or later** for full PG reports. Older engines
  are supported in a fallback mode that reports label counts only (no
  per-label properties or edge source/target).

## Getting started

```bash
# Clone the parent repository and navigate into this package's directory
git clone https://github.com/awslabs/amazon-neptune-tools.git
cd amazon-neptune-tools/neptune-schema-stats

# Install
pip install -e .

# Property graph (auto-detected)
neptune-schema-stats \
    --endpoint my-cluster.us-east-1.neptune.amazonaws.com \
    --iam --region us-east-1

# If the cluster's cached PG statistics or schema are stale, or on a
# freshly loaded cluster where statistics haven't been computed yet,
# rebuild them and continue with the report in one command:
neptune-schema-stats --endpoint <host> --iam --region us-east-1 \
    --refresh

# RDF
neptune-schema-stats --endpoint <host> --iam --region us-east-1 --mode rdf
```

## Sample output

Both examples below use Kelvin Lawrence's public [air-routes](https://github.com/krlawrence/graph) dataset.

### Property graph

```
Property Graph Statistics
=========================
Endpoint:      air-routes.neptune.example.com
Total nodes:             3,748
Total edges:            51,300

Node labels
-----------
Label              Count  Properties
---------  -------------  -------------------------------------------------------------------------------
airport    3,503 — 3,586  city, code, country, desc, elev, icao, lat, lon, longest, region, runways, type
continent        0 — 244  code, desc, type
country          0 — 244  code, desc, type
version           1 — 84  author, code, date, desc, type

Edge labels
-----------
Edge label      Count    Mean/src  Source → Target               Properties
------------  -------  ----------  ----------------------------  ------------
route          50,532           —  airport → airport             dist
contains          768           —  continent, country → airport  —

  Some counts are ranges (min — max). Scans were skipped (--api-only). Re-run without --api-only to resolve them to exact values.
```

Ranges appear because `airport`, `continent`, and `country` all allow characteristic sets like `{code, desc, type}` — a property signature that maps to more than one label in the schema. The default run (without `--api-only`) issues scoped `MATCH (n:L) RETURN count(n)` queries to resolve these to exact counts.

### RDF

```
RDF Graph Statistics
====================
Endpoint:            air-routes.neptune.example.com
Distinct subjects:            54,403
Distinct predicates:              19
Quads (triples):             158,571
Declared classes:                  4

Subject typing
--------------
Category                Count    % of total
--------------------  -------  ------------
Typed (has rdf:type)    3,747          6.9%
Untyped                50,656         93.1%

Class distribution (from SPARQL class-count probe)
--------------------------------------------------
Class        Subjects    % of typed  URI
---------  ----------  ------------  ---------------------------------------------
Airport         3,502         93.5%  http://example.org/air-routes/class/Airport
Country           237          6.3%  http://example.org/air-routes/class/Country
Continent           7          0.2%  http://example.org/air-routes/class/Continent
Version             1          0.0%  http://example.org/air-routes/class/Version

Predicates
----------
Local name      Occurrences    % of quads  URI
------------  -------------  ------------  ------------------------------------------------------
dist                 50,656         31.9%  http://example.org/air-routes/datatypeProperty/dist
route                50,656         31.9%  http://example.org/air-routes/objectProperty/route
contains              7,004          4.4%  http://example.org/air-routes/objectProperty/contains
code                  3,747          2.4%  http://example.org/air-routes/datatypeProperty/code
type                  3,747          2.4%  http://www.w3.org/1999/02/22-rdf-syntax-ns#type
label                 3,747          2.4%  http://www.w3.org/2000/01/rdf-schema#label
…
```

RDF mode reports total subjects/predicates/quads, splits subjects into typed vs untyped (based on `rdf:type` presence), breaks down typed subjects by class, and lists predicates by occurrence count.

## More

- `neptune-schema-stats --help` — full CLI reference

## Contributing

Contributions welcome. See the parent repository's
[CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

Development setup:

```bash
pip install -e ".[dev]"
pytest          # run tests
ruff check .    # lint
ruff format .   # format
```

## License

Apache-2.0. See the parent repository's [LICENSE](../LICENSE) file.
