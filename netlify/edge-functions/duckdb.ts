import { Application, Router } from "https://deno.land/x/oak@v12.6.1/mod.ts";
// @deno-types="./duckdb.types.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME, DuckDBBindings, DuckDBConnection } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const ddb: DuckDBBindings = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);

await ddb.instantiate(() => { });

const res = await fetch('https://github.com/mnsrulz/hpqdata/releases/download/v1.0/db.parquet');
ddb.registerFileBuffer('db.parquet', new Uint8Array(await res.arrayBuffer()));
const conn: DuckDBConnection = ddb.connect();

// Deno.serve(() => {
//   const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
//                                               FROM 'db.parquet'
//                                               --WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
//                                               `)
//   const result = arrowResult.toArray().map((row: any) => row.toJSON());
//   return Response.json(result[0]);
// });
declare type DataQuery = {page: number, page_size: number, status: string, start_date: string, end_date: string}
const router = new Router();
router
  .get("/counts", (context) => {
    const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              --WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20'
                                              `);
    context.response.body = arrowResult.toArray().map((row: any) => row.toJSON());
  })
  .get("/approvedCounts", (context) => {
    const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              WHERE CASE_STATUS = 'Approved'
                                              `);
    context.response.body = arrowResult.toArray().map((row: any) => row.toJSON());
  })
  .get("/data", (context) => {
    const qu:any = getQuery(context) as unknown;
    const statusQuery = qu.status && `AND CASE_STATUS = '${qu.status}'`;
    const pageSize = Math.min(qu.page_size || 100);
    const offset = ((qu.page || 1) - 1) * pageSize;
    const baseQuey = `FROM 'db.parquet'
    WHERE 1=1
    ${statusQuery || ''}
    --ORDER BY RECEIVED_DATE DESC
    LIMIT ${pageSize}
    OFFSET ${offset}`
    const arrowResult = conn.query(`SELECT * ${baseQuey}`);
    const countResult = conn.query(`SELECT COUNT(*) AS total_count ${baseQuey}`);
    context.response.body = {
      count: countResult.toArray().map((row: any) => row.toJSON()),
      data: arrowResult.toArray().map((row: any) => row.toJSON())
    };
  })
  .get("/attributes/:attr", (context) => {
    const {attr, q} = getQuery(context, { mergeParams: true });
    const filter = q && `AND ${attr} ILIKE '%${q}%'`;
    const arrowResult = conn.query(`SELECT DISTINCT ${attr} AS name
                                              FROM 'db.parquet'
                                              WHERE 1=1
                                              ${filter || ''}
                                              LIMIT 100
                                              `);
    context.response.body = arrowResult.toArray().map((row: any) => row.toJSON());
  })
  .get("/pastYearCounts", (context) => {
    const arrowResult = conn.query<{total_count: number}>(`SELECT COUNT(*) AS total_count
                                              FROM 'db.parquet'
                                              WHERE RECEIVED_DATE > current_date - 365
                                              `);
    context.response.body = arrowResult.toArray().map((row: any) => row.toJSON());
  });

const app = new Application();
app.use(router.routes());
app.use(router.allowedMethods());

await app.listen({ port: 8000 });