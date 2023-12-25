// @deno-types="./duckdb.types.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME, DuckDBBindings, DuckDBConnection } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const ddb: DuckDBBindings = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);

await ddb.instantiate(() => { });

const res = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet');
ddb.registerFileBuffer('db.parquet', new Uint8Array(await res.arrayBuffer()));
const conn: DuckDBConnection = ddb.connect();

Deno.serve(() => {
  const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              --WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
                                              `)
  const result = arrowResult.toArray().map((row: any) => row.toJSON());
  return Response.json(result[0]);
});