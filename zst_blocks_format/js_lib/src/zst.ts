//import { init, decompress } from'@bokuweb/zstd-wasm';
import { init, decompress, compress } from 'https://deno.land/x/zstd_wasm/deno/zstd.ts';


let initComplete = false;
const initCompletePromise = init().then(() => {
	initComplete = true;
});

export async function zstDecompress(data: Buffer): Promise<Buffer> {
	if (!initComplete)
		await initCompletePromise;
//    console.log(decompress(data));
//    return Buffer.from(decompress(data));
    return new Uint8Array(decompress(data));
}
