// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
import { Int32 } from 'https://esm.sh/v135/apache-arrow@13.0.0/type.js';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

const arrayBuffer = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet')    //let's initialize the data set in memory
    .then(r => r.arrayBuffer());
db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));

//HTTP paths are not supported due to xhr not available in deno.
//db.registerFileURL('db.parquet', 'https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet', DuckDBDataProtocol.HTTP, false);

const conn = db.connect();

export default function handler() {
    const arrowResult = conn.query<{ total_count: Int32 }>(`SELECT COUNT(*) AS total_count FROM 'db.parquet'`);
    return Response.json({
        total_count: arrowResult.get(0)?.total_count
    });
}

export const config = {
    path: "/duckdb"
};