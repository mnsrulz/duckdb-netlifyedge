import { conn } from '../../services/db.ts'
import { md5 } from 'hash-wasm';
//BigInt.prototype.toJSON = function() { return this.toString() }
BigInt.prototype.toJSON = function() { return Number(this); }    //to keep them as numbers. Numbers have good range.
const map = new Map<string, object>();
const instanceId = crypto.randomUUID()
export default async function handler(req: Request) {
    try {
        const q = new URL(req.url).searchParams.get('q');
        if(!q) throw new Error(`empty query provided. Use with ?q=YOUR_QUERY`)
        const qmd5 = await md5(q);
        if (map.get(qmd5)) return Response.json(map.get(qmd5), {
          headers: {
            'Access-Control-Allow-Origin': '*',
            'x-read-from': 'cache-map',
            'x-instance-id': instanceId,
            'x-md5': qmd5
          }
        });
        
        const arrowResult = conn.query(q);
        const result = arrowResult.toArray().map((row) => row.toJSON());
        map.set(qmd5, result);
        return Response.json(result, {
            headers: {
                'Access-Control-Allow-Origin': '*',
                'x-instance-id': instanceId,
                'x-md5': qmd5
            }
        });
    } catch (err) {
         return Response.json({ error: "Unhandled error occurred", message: err.message }, {
            status: 500,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'x-instance-id': instanceId
            }
        });
    }
}

export const config = {
    path: "/raw"
};
