import { random } from '../../services/shared.ts'
export default function handler() {
    return Response.json({
        random
    });
}

export const config = {
    path: "/f2"
};