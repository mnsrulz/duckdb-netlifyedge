import { Application, Router } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const ddb = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);

await ddb.instantiate(() => { });

const res = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet');
ddb.registerFileBuffer('db.parquet', new Uint8Array(await res.arrayBuffer()));
const conn = ddb.connect();

export default async function handler(req: Request, context: Context) {
    const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              `);
    const result = arrowResult.toArray().map((row: any) => row.toJSON());
    return Response.json(result);
}

export const config: Config = {
    path: "/duckdb",
};
