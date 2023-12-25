// import { getJsDelivrBundles, selectBundle, ConsoleLogger, AsyncDuckDB } from "https://esm.sh/@duckdb/duckdb-wasm@1.28.0";
// import { Int } from "https://esm.sh/v135/apache-arrow@13.0.0/type.js";
// const JSDELIVR_BUNDLES = getJsDelivrBundles();
// const bundle = await selectBundle(JSDELIVR_BUNDLES);
// const worker_url = new URL(bundle.mainWorker!, import.meta.url).href;

// const worker = new Worker(worker_url, { type: 'module' })
// const logger = new ConsoleLogger();
// const db = new AsyncDuckDB(logger, worker);
// await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
// URL.revokeObjectURL(worker_url);

// const res = await fetch('https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet');
// await db.registerFileBuffer('taxi_2019_04.parquet', new Uint8Array(await res.arrayBuffer()));
// BigInt.prototype.toJSON = function () { return this.toString() }

// export default async function handler(req: Request, context: Context) {
  //     const conn = await db.connect();
  //     const arrowResult = await conn.query<{total_count: Int}>(`SELECT COUNT(*) AS total_count
  //                                             FROM 'taxi_2019_04.parquet'
  //                                             WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'`)
  //                                       //arrowResult.get(0)?.total_count      
  //     const result = arrowResult.toArray().map((row) => row.toJSON());
  //     return Response.json(result[0]);
  // }
  
  // export const config: Config = {
    //     path: "/duckdb",
    // };
    
//import pl from "npm:nodejs-polars";
//import pl from "https://esm.sh/nodejs-polars";
// @deno-types="https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-node-blocking.d.ts"
//import { createDuckDB, getJsDelivrBundles, ConsoleLogger, NODE_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-node-blocking.cjs';

import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const ddb = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await ddb.instantiate();

//const res = await fetch('https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet');
const res = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet');
ddb.registerFileBuffer('db.parquet', new Uint8Array(await res.arrayBuffer()));

Deno.serve(async (req) => {

      const conn = await ddb.connect();
      const arrowResult = await conn.query(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              --WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
                                              `)
                                        //arrowResult.get(0)?.total_count      
      const result = arrowResult.toArray().map((row:any) => row.toJSON());
      return Response.json(result[0]);

  // return new Response(JSON.stringify(h), {
  //     status: 200,
  //     headers: {
  //       "content-type": "text/plain; charset=utf-8",
  //     },
  //   });
    // const worker = new Worker(
    //     new URL("./worker.ts", import.meta.url).href,
    //     {
    //       type: "module",
    //     },
    //   );
      
    //   worker.postMessage({ filename: "./log.txt" });

    // const conn = await db.connect();
    // const arrowResult = await conn.query<{total_count: Int}>(`SELECT COUNT(*) AS total_count
    //                                         FROM 'taxi_2019_04.parquet'
    //                                         WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'`)
    //                                   //arrowResult.get(0)?.total_count      
    // const result = arrowResult.toArray().map((row) => row.toJSON());
    //return Response.json(result[0]);
    // console.log("Method:", req.method);
  
    // const url = new URL(req.url);
    // console.log("Path:", url.pathname);
    // console.log("Query parameters:", url.searchParams);
  
    // console.log("Headers:", req.headers);
  
    // if (req.body) {
    //   const body = await req.text();
    //   console.log("Body:", body);
    // }
  
    // return new Response("Hello, world", {
    //   status: 200,
    //   headers: {
    //     "content-type": "text/plain; charset=utf-8",
    //   },
    // });
  });