import zlib from 'zlib';

function toHex(buffer: ArrayBuffer): string {
	return [...new Uint8Array(buffer)].map((x) => x.toString(16).padStart(2, '0')).join('');
}

function epoch(): number {
	return Math.floor(Date.now() / 1000);
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
		decompressed_bytesize: content.length,
		bytesize: compressed.length,
		chunk: metadata[3].split('.')[0],
		subchunk: index,
		subchunk_size: batch.length,
		md5: await md5(compressed),
		filename: metadata[2],
		type: metadata[1],
		created_at: epoch(),
	};
	await Promise.all([env.MIZU_CMC_V2.put(new_key, compressed), env.KV.put(new_key, JSON.stringify(new_metadata))]);
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

		const progress = await env.KV.get(r2_key);
		if (progress !== null) {
			return Response.json({ status: 'already processed' });
		}

		const object = await env.MIZU_CMC.get(r2_key);
		if (object === null) {
			return new Response(`Not found`, {
				status: 404,
			});
		}

		console.log('======= start processing');
		console.log("======= r2 object: ", object.size);

		const content: string = zlib.inflateSync(await object.arrayBuffer()).toString();
		console.log("====== content: ", content.length);
		console.log("====== content: ", content);

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

		console.log("======= batches", batches.length);
		const totalWrites = Math.floor(batches.length / 3);
		for (let i = 0; i < totalWrites; i++) {
			const startIndex = i * 3;
			await Promise.all([
				save_chunk(r2_key, batches[startIndex], startIndex, env),
				save_chunk(r2_key, batches[startIndex + 1], startIndex + 1, env),
				save_chunk(r2_key, batches[startIndex + 2], startIndex + 2, env),
			]);
		}
		if (batches.length % 3 > 0) {
			const promises = [];
			for (let i = totalWrites * 3; i < batches.length; i++) {
				promises.push(save_chunk(r2_key, batches[i], i, env));
			}
			await Promise.all(promises);
		}

		console.log('======= recording progress');
		// await env.KV.put(
		// 	r2_key,
		// 	JSON.stringify({
		// 		status: 'processed',
		// 		processed_at: epoch(),
		// 	})
		// );
		return Response.json({ status: 'ok' });
	},
} satisfies ExportedHandler<Env>;
