# Changelog

## [0.3.0](https://github.com/lakeops-org/queryflux/compare/queryflux-v0.2.0...queryflux-v0.3.0) (2026-08-19)


### Features

* ADBC tokenExchange for Snowflake via per-identity sub-pool ([#166](https://github.com/lakeops-org/queryflux/issues/166)) ([db88f95](https://github.com/lakeops-org/queryflux/commit/db88f95fae6ff5a050490cf0af02d2bb09556651))
* add deny routing rules with Denied query history ([#160](https://github.com/lakeops-org/queryflux/issues/160)) ([30c77b8](https://github.com/lakeops-org/queryflux/commit/30c77b854b60361e0fed051cf53a0bd62b5501f5))
* Add helm chart for QueryFlux ([#75](https://github.com/lakeops-org/queryflux/issues/75)) ([3c3acd4](https://github.com/lakeops-org/queryflux/commit/3c3acd4e8f5a3a4e9d46ad873ff8a88413c26d0a))
* add native CLI flow and duckdb quickstart example ([#98](https://github.com/lakeops-org/queryflux/issues/98)) ([2b04b83](https://github.com/lakeops-org/queryflux/commit/2b04b838824a8e832f24fa4919d5b15f8b9d632c))
* Add Python and webhook guardrail execution ([#81](https://github.com/lakeops-org/queryflux/issues/81)) ([0af87cd](https://github.com/lakeops-org/queryflux/commit/0af87cdb8f94e67da2408e8dec46728488c9b944))
* add query result cache with OpenDAL storage backend ([#90](https://github.com/lakeops-org/queryflux/issues/90)) ([5109d49](https://github.com/lakeops-org/queryflux/commit/5109d495b4c7e4971ce58cc7795657093bcdbce0))
* authorization-aware first-fit when routing uses fallback ([#161](https://github.com/lakeops-org/queryflux/issues/161)) ([703ae4a](https://github.com/lakeops-org/queryflux/commit/703ae4a012725710b7ddb3d4931ca2a05516225e))
* cancel in-flight sync queries on client disconnect ([#120](https://github.com/lakeops-org/queryflux/issues/120)) ([e1c1794](https://github.com/lakeops-org/queryflux/commit/e1c17942675d68ac2eb085ee25299320364a8739))
* ClickHouse backend adapter (HTTP, Arrow result path) ([#105](https://github.com/lakeops-org/queryflux/issues/105)) ([60986c1](https://github.com/lakeops-org/queryflux/commit/60986c1571c20982d4789c920a3c55171fc6a7bc))
* ClickHouse impersonate via EXECUTE AS ([#167](https://github.com/lakeops-org/queryflux/issues/167)) ([7d564d7](https://github.com/lakeops-org/queryflux/commit/7d564d73295c0c257a6440f1a6ffb593ec2bc04a))
* distributed multi replica ([#84](https://github.com/lakeops-org/queryflux/issues/84)) ([1b55ced](https://github.com/lakeops-org/queryflux/commit/1b55cedba3c27a61e673b77ce2e7e50c4eaf19a4))
* extend passthrough enrichment to the sync-bridge dispatch path ([#165](https://github.com/lakeops-org/queryflux/issues/165)) ([85f1170](https://github.com/lakeops-org/queryflux/commit/85f117066ae0203f346505f50b42493310e560d0))
* group default tags, admin OpenAPI spec, and Studio Swagger UI ([#78](https://github.com/lakeops-org/queryflux/issues/78)) ([4a97a25](https://github.com/lakeops-org/queryflux/commit/4a97a254de880958479098290a907aa51a5ddb6b))
* **security:** persist query owner and allow operator/admin cancel ([#121](https://github.com/lakeops-org/queryflux/issues/121)) ([7118e70](https://github.com/lakeops-org/queryflux/commit/7118e70567288ca70716d4ef69f0c24acf17156c))
* StarRocks passthrough via LDAP COM_CHANGE_USER ([#168](https://github.com/lakeops-org/queryflux/issues/168)) ([9c054ad](https://github.com/lakeops-org/queryflux/commit/9c054ad82b314657d340af8d45110ca5e4b82c70))
* switch schema migrations to Refinery and add migrate CLI ([#163](https://github.com/lakeops-org/queryflux/issues/163)) ([44ddd03](https://github.com/lakeops-org/queryflux/commit/44ddd0333aa79355a86512b96c89f603a558d7f8))
* wire backend identity into Trino adapter ([#164](https://github.com/lakeops-org/queryflux/issues/164)) ([a70cf1a](https://github.com/lakeops-org/queryflux/commit/a70cf1ad832e77ccdebc8b358f71510b322f8098))


### Bug Fixes

* **athena:** bound wait_for_completion and refresh RoleArn credentials ([#133](https://github.com/lakeops-org/queryflux/issues/133)) ([c2b6c78](https://github.com/lakeops-org/queryflux/commit/c2b6c78fd8d28d6dc2a13e05b985b73fa63168ff))
* **auth:** enforce auth for frontend SET and metadata fast paths ([#127](https://github.com/lakeops-org/queryflux/issues/127)) ([af63da2](https://github.com/lakeops-org/queryflux/commit/af63da2cf3d8ff796eca841f4ad195b43eca0507))
* **auth:** require OIDC audience when auth.required=true ([#124](https://github.com/lakeops-org/queryflux/issues/124)) ([e0773e9](https://github.com/lakeops-org/queryflux/commit/e0773e961fe963360c2a78b6193fd82d0da74e6c))
* bound capacity wait with capacityWaitTimeoutSecs ([#140](https://github.com/lakeops-org/queryflux/issues/140)) ([b6335c2](https://github.com/lakeops-org/queryflux/commit/b6335c270a387815075dbe7c293489c7829bb8ce))
* cancel in-flight Snowflake HTTP and SQL API queries (P2-39) ([#156](https://github.com/lakeops-org/queryflux/issues/156)) ([3b0c656](https://github.com/lakeops-org/queryflux/commit/3b0c6561e0595a2d26fbc0c2b7b65b8d428b35d1))
* cancel Trino queries with cluster credentials before releasing slots ([#131](https://github.com/lakeops-org/queryflux/issues/131)) ([a517b3c](https://github.com/lakeops-org/queryflux/commit/a517b3c129d10f0e64f2542b1d572cbe9941c3f6))
* cancel zombie/admin queries through adapters with cluster credentials ([#134](https://github.com/lakeops-org/queryflux/issues/134)) ([e35b709](https://github.com/lakeops-org/queryflux/commit/e35b70937aacd9b192717d67c2ff817a4747addf))
* cap MySQL and Postgres wire message sizes before allocate ([#143](https://github.com/lakeops-org/queryflux/issues/143)) ([0f03017](https://github.com/lakeops-org/queryflux/commit/0f03017df27f2d31abab01a4aa173a7515f2de6c))
* count config reload and auth rebuild failures ([#138](https://github.com/lakeops-org/queryflux/issues/138)) ([1f86894](https://github.com/lakeops-org/queryflux/commit/1f86894fa6b66b08de84a9262be54e76e37a0596))
* do not apply a 30s timeout to Trino poll GETs ([#129](https://github.com/lakeops-org/queryflux/issues/129)) ([802fe73](https://github.com/lakeops-org/queryflux/commit/802fe736a07fa89d6094fe8c592a164ce84d29ae))
* enforce maxQueuedQueries from LiveConfig ([#135](https://github.com/lakeops-org/queryflux/issues/135)) ([7276e16](https://github.com/lakeops-org/queryflux/commit/7276e168bd9c2a72d8db4a0b66b1c49aa3d07a37))
* fail-closed guard kinds (script + webhook) ([#162](https://github.com/lakeops-org/queryflux/issues/162)) ([e725b17](https://github.com/lakeops-org/queryflux/commit/e725b17136db3c7ed8c326855f200bdee3561475))
* heartbeat queue claims so slow dispatch cannot double-run ([#130](https://github.com/lakeops-org/queryflux/issues/130)) ([7055e6d](https://github.com/lakeops-org/queryflux/commit/7055e6d0ca71e82b8deec377a7f1dc96c727ba47))
* **helm:** reject empty NetworkPolicy that would lock out traffic ([#142](https://github.com/lakeops-org/queryflux/issues/142)) ([71a06c8](https://github.com/lakeops-org/queryflux/commit/71a06c8a8e629f4468ed2ddfcd06c64d2652fd5c))
* **helm:** startupProbe, terminationGracePeriod, and appVersion 0.2.0 ([#137](https://github.com/lakeops-org/queryflux/issues/137)) ([b589b2d](https://github.com/lakeops-org/queryflux/commit/b589b2d0657e99bc3004845dcbc5abeba00148b6))
* honor frontends.trinoHttp.enabled when spawning the listener ([#144](https://github.com/lakeops-org/queryflux/issues/144)) ([c63be4e](https://github.com/lakeops-org/queryflux/commit/c63be4e16a50e6ae5caceb5404746c6e47e7ed7a))
* increment auth_failures_total on frontend auth errors ([#136](https://github.com/lakeops-org/queryflux/issues/136)) ([45bd962](https://github.com/lakeops-org/queryflux/commit/45bd96242ef11fc56b1805e0fd6521ef7047a9dd))
* install UserGroup router from config ([#146](https://github.com/lakeops-org/queryflux/issues/146)) ([457cca8](https://github.com/lakeops-org/queryflux/commit/457cca81bd3b931978742f20183ed6c701faaa68))
* isolate Postgres pools for query, coordination, and admin (P1-23) ([#155](https://github.com/lakeops-org/queryflux/issues/155)) ([893bd89](https://github.com/lakeops-org/queryflux/commit/893bd89c5c7d29bf4ce6dfcdf0c272efd8d973e4))
* persist admin password changes in Postgres ([#123](https://github.com/lakeops-org/queryflux/issues/123)) ([9b332ee](https://github.com/lakeops-org/queryflux/commit/9b332eebfde8da208fc8a978c2cf42c5fc037d2e))
* persist Studio security settings ([#99](https://github.com/lakeops-org/queryflux/issues/99)) ([#122](https://github.com/lakeops-org/queryflux/issues/122)) ([ce5bb18](https://github.com/lakeops-org/queryflux/commit/ce5bb187643b8b9dbae8a0819c67a5a0288ad432))
* purge digests and cluster snapshots with query history retention ([#145](https://github.com/lakeops-org/queryflux/issues/145)) ([2124a16](https://github.com/lakeops-org/queryflux/commit/2124a163383fb49b93ef9633ec040f9302fe036e))
* record query history and dashboard stats with in-memory persistence ([#94](https://github.com/lakeops-org/queryflux/issues/94)) ([bd0490a](https://github.com/lakeops-org/queryflux/commit/bd0490a6d23e253a56e760e1a496fac2be5cc240))
* record query history on cancel and queue terminal paths ([#150](https://github.com/lakeops-org/queryflux/issues/150)) ([e105824](https://github.com/lakeops-org/queryflux/commit/e10582488bea1c4316c2f1beedfda641b9e56b0a))
* reject unimplemented Redis persistence at startup ([#147](https://github.com/lakeops-org/queryflux/issues/147)) ([5f61fd8](https://github.com/lakeops-org/queryflux/commit/5f61fd87999a321ac46f0f10a701a9b7c64b3d40))
* seed YAML clusters into Postgres only when missing ([#151](https://github.com/lakeops-org/queryflux/issues/151)) ([02c31b1](https://github.com/lakeops-org/queryflux/commit/02c31b11d31e6125418819583c647412f21385bd))
* stop background tasks when shutdown is signaled ([#148](https://github.com/lakeops-org/queryflux/issues/148)) ([c571ef6](https://github.com/lakeops-org/queryflux/commit/c571ef6a05f63d918f3d82fe236b4aa0f18dbf3b))
* stream DuckDB results with pool and buffer cap (P1-18) ([#154](https://github.com/lakeops-org/queryflux/issues/154)) ([5dfce7c](https://github.com/lakeops-org/queryflux/commit/5dfce7cee4241dd508d866837d2292e7e8a0a8c0))
* stream StarRocks/MySQL-native results (P1-17) ([#153](https://github.com/lakeops-org/queryflux/issues/153)) ([aca2219](https://github.com/lakeops-org/queryflux/commit/aca2219dc5c93006801ed69f1c5298449a753f78))
* **trino:** cancel or drain catalog helper queries so executions do not leak ([#132](https://github.com/lakeops-org/queryflux/issues/132)) ([7c0f61d](https://github.com/lakeops-org/queryflux/commit/7c0f61d618114171722962981e0793335cf8ce95))
* **trino:** escape identifiers in discovery queries ([#125](https://github.com/lakeops-org/queryflux/issues/125)) ([08cdfc8](https://github.com/lakeops-org/queryflux/commit/08cdfc8c6b7141c3d6698397c1a9c72aa2bf0458))
* **trino:** prevent client Authorization from overriding cluster auth ([#126](https://github.com/lakeops-org/queryflux/issues/126)) ([e994ade](https://github.com/lakeops-org/queryflux/commit/e994ade5974eee61dd3fc88f239c1a2a91ef266e))
* validate auth and OpenFGA config at startup when required ([#149](https://github.com/lakeops-org/queryflux/issues/149)) ([bb473d9](https://github.com/lakeops-org/queryflux/commit/bb473d930044d11baf8c723d0b4302c411bb6d93))
* website/package.json & website/package-lock.json to reduce vulnerabilities ([#110](https://github.com/lakeops-org/queryflux/issues/110)) ([bfce084](https://github.com/lakeops-org/queryflux/commit/bfce084c7dd9a2dd34c46203822d27c10352bd6d))

## [0.2.0](https://github.com/lakeops-org/queryflux/compare/queryflux-v0.1.2...queryflux-v0.2.0) (2026-06-02)


### Features

* support agentic ai + gaurd rails  ([#72](https://github.com/lakeops-org/queryflux/issues/72)) ([6aa5adb](https://github.com/lakeops-org/queryflux/commit/6aa5adbff5dfb622d32f5d4a12c59db6577ff405))


### Bug Fixes

* **deployment:** admin api auth ([#61](https://github.com/lakeops-org/queryflux/issues/61)) ([5bc377a](https://github.com/lakeops-org/queryflux/commit/5bc377ae0c5c76ac63c06f2c5b49dedbb585c762))

## [0.1.2](https://github.com/lakeops-org/queryflux/compare/queryflux-v0.1.1...queryflux-v0.1.2) (2026-04-15)


### ⚠ BREAKING CHANGES

* initial 0.1.0 release

### Features

* adbc ([#23](https://github.com/lakeops-org/queryflux/issues/23)) ([6f7464b](https://github.com/lakeops-org/queryflux/commit/6f7464ba0698de48f9b50d96fd0630fb68da1e8f))
* add  support for tags ([#13](https://github.com/lakeops-org/queryflux/issues/13)) ([9e19f07](https://github.com/lakeops-org/queryflux/commit/9e19f07b260bbde400ac5d6965aa46f005052546))
* add auth for admin api ([#20](https://github.com/lakeops-org/queryflux/issues/20)) ([af77143](https://github.com/lakeops-org/queryflux/commit/af77143107b22cd29caf1fa9ce31ec1a401925b5))
* add deploy for website ([#9](https://github.com/lakeops-org/queryflux/issues/9)) ([18b21d6](https://github.com/lakeops-org/queryflux/commit/18b21d62d13506156bbb0ff221e9275f894bf2e4))
* Custom domain ([#10](https://github.com/lakeops-org/queryflux/issues/10)) ([1518c40](https://github.com/lakeops-org/queryflux/commit/1518c40052d050bfc73e085ccc4a3184b5a2dc08))
* initial 0.1.0 release ([712a135](https://github.com/lakeops-org/queryflux/commit/712a1352be2c577c8d3bf9ab5a46124befda7d26))
* Light mode ([#12](https://github.com/lakeops-org/queryflux/issues/12)) ([9d49e00](https://github.com/lakeops-org/queryflux/commit/9d49e0020ae85a303dbd480b0fb92625649b0513))
* **main:** release queryflux 0.1.1 ([#4](https://github.com/lakeops-org/queryflux/issues/4)) ([04d9f48](https://github.com/lakeops-org/queryflux/commit/04d9f48797e764e31b4c585814a03b6953818a37))
* query parameters end-to-end and Snowflake-compatible APIs ([#56](https://github.com/lakeops-org/queryflux/issues/56)) ([998f29c](https://github.com/lakeops-org/queryflux/commit/998f29c28e8239ddd32541910d77ce8d3197ed25))
* Website changes  ([#8](https://github.com/lakeops-org/queryflux/issues/8)) ([4145829](https://github.com/lakeops-org/queryflux/commit/4145829d705c94ce0384de0cd70d82dd38e4b597))


### Bug Fixes

* **ci:** libduckdb workflow, Makefile targets, cargo linker config ([#16](https://github.com/lakeops-org/queryflux/issues/16)) ([4291f8c](https://github.com/lakeops-org/queryflux/commit/4291f8cdae851fec39e12c18acdfa5ae109e8c91))
* connection to starrocks + ui fixes + add backend to ui ([#7](https://github.com/lakeops-org/queryflux/issues/7)) ([a65eeb0](https://github.com/lakeops-org/queryflux/commit/a65eeb08462622b6b9250d381c26d40cf2a4095c))
* Custom domain ([#11](https://github.com/lakeops-org/queryflux/issues/11)) ([2971e1f](https://github.com/lakeops-org/queryflux/commit/2971e1f0df3e9903fb213959727e0b2dc7942cfe))
* docker build ([#6](https://github.com/lakeops-org/queryflux/issues/6)) ([5b8ac7d](https://github.com/lakeops-org/queryflux/commit/5b8ac7d6768a82fa4a4b864361379fb1fd95b3a4))
* docs ([#21](https://github.com/lakeops-org/queryflux/issues/21)) ([085ee22](https://github.com/lakeops-org/queryflux/commit/085ee227b15c94b7a7f46c8a858b2a7d2fae5de3))
* don't convert to arrow when frontend and backend talk same protocol ([#53](https://github.com/lakeops-org/queryflux/issues/53)) ([472640a](https://github.com/lakeops-org/queryflux/commit/472640a27a02fe880d3cae9601bfcaaa5859e428))
* make builds work + add pr title checker ([#5](https://github.com/lakeops-org/queryflux/issues/5)) ([9f14137](https://github.com/lakeops-org/queryflux/commit/9f14137b04ba0b29c18cd7dfa8364f109da7462e))
* mobile navbar hamburger menu and sidebar drawer ([#49](https://github.com/lakeops-org/queryflux/issues/49)) ([fb1a2bd](https://github.com/lakeops-org/queryflux/commit/fb1a2bde8046ecdefd50d9e785989f0241bc5137))
* point favicon and OG image to existing hero banner, bump hero text sizes ([#52](https://github.com/lakeops-org/queryflux/issues/52)) ([f457276](https://github.com/lakeops-org/queryflux/commit/f4572761730d61ebfd05780fd16a27b91a7bb816))
* Split/platform no snowflake ([#17](https://github.com/lakeops-org/queryflux/issues/17)) ([360a255](https://github.com/lakeops-org/queryflux/commit/360a25572d5c0be097c4252201ad3aa8013b01d3))
* Trino basic auth, Studio StarRocks poolSize, docs, and DB safety ([#19](https://github.com/lakeops-org/queryflux/issues/19)) ([83cf1ec](https://github.com/lakeops-org/queryflux/commit/83cf1ecf2977ef7fcc70cf8724199b94cf270121))


### Miscellaneous Chores

* release 0.1.2 ([#58](https://github.com/lakeops-org/queryflux/issues/58)) ([4c9f924](https://github.com/lakeops-org/queryflux/commit/4c9f924404a324863bdff6d7555a09e36c1ce7f6))

## [0.1.2](https://github.com/lakeops-org/queryflux/compare/queryflux-v0.1.1...queryflux-v0.1.2) (2026-04-15)

### Features

* query parameters end-to-end and Snowflake-compatible APIs ([#56](https://github.com/lakeops-org/queryflux/issues/56)) ([998f29c](https://github.com/lakeops-org/queryflux/commit/998f29c28e8239ddd32541910d77ce8d3197ed25))

### Miscellaneous Chores

* release 0.1.2 ([#58](https://github.com/lakeops-org/queryflux/pull/58)) ([4c9f924](https://github.com/lakeops-org/queryflux/commit/4c9f924404a324863bdff6d7555a09e36c1ce7f6))

## [0.1.1](https://github.com/lakeops-org/queryflux/compare/queryflux-v0.1.0...queryflux-v0.1.1) (2026-04-15)

### Features

* adbc ([#23](https://github.com/lakeops-org/queryflux/issues/23)) ([6f7464b](https://github.com/lakeops-org/queryflux/commit/6f7464ba0698de48f9b50d96fd0630fb68da1e8f))
* add  support for tags ([#13](https://github.com/lakeops-org/queryflux/issues/13)) ([9e19f07](https://github.com/lakeops-org/queryflux/commit/9e19f07b260bbde400ac5d6965aa46f005052546))
* add auth for admin api ([#20](https://github.com/lakeops-org/queryflux/issues/20)) ([af77143](https://github.com/lakeops-org/queryflux/commit/af77143107b22cd29caf1fa9ce31ec1a401925b5))
* add deploy for website ([#9](https://github.com/lakeops-org/queryflux/issues/9)) ([18b21d6](https://github.com/lakeops-org/queryflux/commit/18b21d62d13506156bbb0ff221e9275f894bf2e4))
* Custom domain ([#10](https://github.com/lakeops-org/queryflux/issues/10)) ([1518c40](https://github.com/lakeops-org/queryflux/commit/1518c40052d050bfc73e085ccc4a3184b5a2dc08))
* initial 0.1.0 release ([712a135](https://github.com/lakeops-org/queryflux/commit/712a1352be2c577c8d3bf9ab5a46124befda7d26))
* Light mode ([#12](https://github.com/lakeops-org/queryflux/issues/12)) ([9d49e00](https://github.com/lakeops-org/queryflux/commit/9d49e0020ae85a303dbd480b0fb92625649b0513))
* Website changes  ([#8](https://github.com/lakeops-org/queryflux/issues/8)) ([4145829](https://github.com/lakeops-org/queryflux/commit/4145829d705c94ce0384de0cd70d82dd38e4b597))


### Bug Fixes

* **ci:** libduckdb workflow, Makefile targets, cargo linker config ([#16](https://github.com/lakeops-org/queryflux/issues/16)) ([4291f8c](https://github.com/lakeops-org/queryflux/commit/4291f8cdae851fec39e12c18acdfa5ae109e8c91))
* connection to starrocks + ui fixes + add backend to ui ([#7](https://github.com/lakeops-org/queryflux/issues/7)) ([a65eeb0](https://github.com/lakeops-org/queryflux/commit/a65eeb08462622b6b9250d381c26d40cf2a4095c))
* Custom domain ([#11](https://github.com/lakeops-org/queryflux/issues/11)) ([2971e1f](https://github.com/lakeops-org/queryflux/commit/2971e1f0df3e9903fb213959727e0b2dc7942cfe))
* docker build ([#6](https://github.com/lakeops-org/queryflux/issues/6)) ([5b8ac7d](https://github.com/lakeops-org/queryflux/commit/5b8ac7d6768a82fa4a4b864361379fb1fd95b3a4))
* docs ([#21](https://github.com/lakeops-org/queryflux/issues/21)) ([085ee22](https://github.com/lakeops-org/queryflux/commit/085ee227b15c94b7a7f46c8a858b2a7d2fae5de3))
* don't convert to arrow when frontend and backend talk same protocol ([#53](https://github.com/lakeops-org/queryflux/issues/53)) ([472640a](https://github.com/lakeops-org/queryflux/commit/472640a27a02fe880d3cae9601bfcaaa5859e428))
* make builds work + add pr title checker ([#5](https://github.com/lakeops-org/queryflux/issues/5)) ([9f14137](https://github.com/lakeops-org/queryflux/commit/9f14137b04ba0b29c18cd7dfa8364f109da7462e))
* mobile navbar hamburger menu and sidebar drawer ([#49](https://github.com/lakeops-org/queryflux/issues/49)) ([fb1a2bd](https://github.com/lakeops-org/queryflux/commit/fb1a2bde8046ecdefd50d9e785989f0241bc5137))
* point favicon and OG image to existing hero banner, bump hero text sizes ([#52](https://github.com/lakeops-org/queryflux/issues/52)) ([f457276](https://github.com/lakeops-org/queryflux/commit/f4572761730d61ebfd05780fd16a27b91a7bb816))
* Split/platform no snowflake ([#17](https://github.com/lakeops-org/queryflux/issues/17)) ([360a255](https://github.com/lakeops-org/queryflux/commit/360a25572d5c0be097c4252201ad3aa8013b01d3))
* Trino basic auth, Studio StarRocks poolSize, docs, and DB safety ([#19](https://github.com/lakeops-org/queryflux/issues/19)) ([83cf1ec](https://github.com/lakeops-org/queryflux/commit/83cf1ecf2977ef7fcc70cf8724199b94cf270121))
