import zlib from 'zlib';

interface Chunk {
	key: string;
	compressed: string;
	metadata: {
		batch: string;
		decompressed_bytesize: number;
		bytesize: number;
		chunk: string;
		subchunk: number;
		subchunk_size: number;
		md5: string;
		filename: string;
		type: string;
		created_at: number;
	}
}

function toHex(buffer: ArrayBuffer): string {
	return [...new Uint8Array(buffer)].map((x) => x.toString(16).padStart(2, '0')).join('');
}

async function md5(data: string): Promise<string> {
	const encoded = new TextEncoder().encode(data);
	const myDigest = await crypto.subtle.digest(
		{
			name: 'MD5',
		},
		encoded
	);

	return toHex(myDigest);
}

async function save_chunk(r2_key: string, batch: Array<string>, index: number, env: Env) {
	const content = batch.join('\n');
	const compressed = zlib.deflateSync(content).toString('base64');
	const metadata = r2_key.split('/');
	const new_key = r2_key + '/' + index + '.zz';
	const new_metadata = {
		batch: metadata[0],
		decompressed_bytesize: content.length.toString(),
		bytesize: compressed.length.toString(),
		chunk: metadata[3].split('.')[0],
		subchunk: index.toString(),
		subchunk_size: batch.length.toString(),
		md5: await md5(compressed),
		filename: metadata[2],
		type: metadata[1],
		created_at: new Date().toISOString(),
	}
	await env.MIZU_CMC_V2.put(new_key, compressed, {
		customMetadata: new_metadata,
	});
	return new_metadata;
}

export default {
	async fetch(request, env): Promise<Response> {
		const url = new URL(request.url);
		const r2_key = url.searchParams.get('r2_key');
		if (r2_key === null) {
			return new Response(`r2_key is required`, {
				status: 400,
			});
		}

		const object = await env.MIZU_CMC.get(r2_key);
		if (object === null) {
			return new Response(`Not found`, {
				status: 404,
			});
		}
		const content: string = zlib.inflateSync(await object.arrayBuffer()).toString();
		const batches: Array<Array<string>> = [];
		let batch_cache: Array<string> = [];
		let batch_size: number = 0;

		content.split('\n').forEach((line: string) => {
			batch_cache.push(line);
			batch_size += line.length;
			// > 500kb
			if (batch_size > 500 * 1000) {
				batches.push(batch_cache);
				batch_cache = [];
				batch_size = 0;
			}
		});
		if (batch_cache.length > 0) {
			batches.push(batch_cache);
		}

		const metadata = await Promise.all(batches.map((b, index) => save_chunk(r2_key, b, index, env)));
		return Response.json({ metadata });
	},
} satisfies ExportedHandler<Env>;
