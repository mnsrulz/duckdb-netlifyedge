import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME, DuckDBDataProtocol } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);

await db.instantiate(() => { });

// const res = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet');
// db.registerFileBuffer('db.parquet', new Uint8Array(await res.arrayBuffer()));

db.registerFileURL('db.parquet', 'https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet', DuckDBDataProtocol.HTTP, false);

const conn = db.connect();

export default async function handler(req: Request, context: Context) {
    const arrowResult = conn.query(`SELECT COUNT(*) AS total_count FROM 'db.parquet'`);
    const result = arrowResult.toArray().map((row: any) => row.toJSON());
    return Response.json(result);
}

export const config: Config = {
    path: "/duckdb",
};