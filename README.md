# Boomerang (WIP)

Send request which should be executed after some time.

Goal is to have 100k requests per second per instance (inserts / calls).

Should support: Sqlite, Postgresql, Cassandra/Scylla

## TODOS:

- [ ] Yaml config
- [ ] Mutli instance support
- [ ] Gorutes per "tag" (or endpoint, one endpoint should not affect other.. maybe some speed control?)

- [ ] Postgresql + Cassandra/Scylla 
- [ ] Tune databases
- [ ] Database sharding
