# OCI connector source

The separate checkout at `oracle-observability/` comes from
https://github.com/Kalsaria-C/kafka-connect-oracleobservability.
Its source revision is pinned in `config/oci-upstream.json` at the project root.

`python3 scripts/oci.py build` clones it when missing, checks the revision,
and runs the upstream Maven verification and packaging steps using Java 17.
The checkout is ignored by the parent project so its Git history and origin
stay independent. The upstream source is not patched by this integration.

When upgrading, select the desired upstream revision, update the pin to that
same commit, rebuild, and reinstall. Local source changes are never overwritten.
