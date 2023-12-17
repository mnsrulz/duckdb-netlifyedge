import * as duckdb from "https://esm.sh/@duckdb/duckdb-wasm@1.28.0";
const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
const worker_url = new URL(bundle.mainWorker!, import.meta.url).href;

const worker = new Worker(worker_url, { type: 'module' })
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

const res = await fetch('https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet');
await db.registerFileBuffer('taxi_2019_04.parquet', new Uint8Array(await res.arrayBuffer()));
BigInt.prototype.toJSON = function () { return this.toString() }

export default async function handler(req: Request, context: Context) {
    const conn = await db.connect();
    const arrowResult = await conn.query(`SELECT COUNT(*) AS total_count
  FROM 'taxi_2019_04.parquet'
  WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'`)
    const stream = JSON.stringify(arrowResult.toArray().map((row) => row.toJSON()));

    return new Response(stream, {
        status: 200,
        headers: { "Content-Type": "text/html" },
    });
}

export const config: Config = {
    path: "/duckdb",
};
