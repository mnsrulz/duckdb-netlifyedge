import { Int32 } from 'https://esm.sh/v135/apache-arrow@13.0.0/type.js';
import { conn } from '../../services/db.ts'
export default function handler() {
    const arrowResult = conn.query<{ total_count: Int32 }>(`SELECT COUNT(*) AS total_count FROM 'db.parquet'`);
    return Response.json({
        total_count: arrowResult.get(0)?.total_count
    });
}

export const config = {
    path: "/duckdb"
};