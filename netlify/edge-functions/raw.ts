import { conn } from '../../services/db.ts'
export default function handler(req: Request) {
    const q = new URL(req.url).searchParams.get('q');
    if(!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
    const arrowResult = conn.query(q);
    const result = arrowResult.toArray().map((row) => row.toJSON());
    return Response.json(result, {
        headers: {
            'Access-Control-Allow-Origin': '*'
        }
    });
}

export const config = {
    path: "/raw"
};