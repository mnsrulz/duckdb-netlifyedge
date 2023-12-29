import { conn } from '../../services/db.ts'
//BigInt.prototype.toJSON = function() { return this.toString() }
BigInt.prototype.toJSON = function() { return Number(this); }    //to keep them as numbers. Numbers have good range.
const map = new Map<string, object>();
export default function handler(req: Request) {
    try {
        const q = new URL(req.url).searchParams.get('q');
        if(!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
        if (map.get(q)) return Response.json(map.get(q), {
          headers: {
            'Access-Control-Allow-Origin': '*'
          }
        });
        
        const arrowResult = conn.query(q);
        const result = arrowResult.toArray().map((row) => row.toJSON());
        map.set(q, result);
        return Response.json(result, {
            headers: {
                'Access-Control-Allow-Origin': '*'
            }
        });
    } catch (err) {
         return Response.json({ error: "Unhandled error occurred", message: err.message }, {
            status: 500,
            headers: {
                'Access-Control-Allow-Origin': '*'
            }
        });
    }
}

export const config = {
    path: "/raw"
};
