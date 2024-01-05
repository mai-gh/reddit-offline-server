import { promises as fsp } from "node:fs";
import { zstDecompress } from "./zst.ts";
import {Buffer} from "https://deno.land/std/io/buffer.ts";

const defaultCompressionLevel = 3;

export class ZstBlocksFile {
	static async readBlockRowAt(file: fsp.FileHandle, position: RowPosition): Promise<Uint8Array> {
		return await ZstBlock.readRowAt(file, position);
	}

	static async readMultipleBlockRowsAt(file: fsp.FileHandle, positions: RowPosition[]): Promise<Uint8Array[]> {
		const blockGroups: { [blockOffset: number]: RowPositionWithIndex[] } = {};
		for (let i = 0; i < positions.length; i++) {
			const position = positions[i];
			if (!blockGroups[position.blockOffset])
				blockGroups[position.blockOffset] = [];
			blockGroups[position.blockOffset].push({
				...position,
				originalIndex: i
			});
		}

		const blockBatches: [number, Uint8Array][][] = await Promise.all(Object.keys(blockGroups).map(async (blockOffsetStr) => {
			const blockOffset = parseInt(blockOffsetStr);
			const blockPositions = blockGroups[blockOffset];
			const readRows = await ZstBlock.readMultipleRowsAt(file, blockOffset, blockPositions);
			return readRows.map((row, i) => [blockPositions[i].originalIndex, row] as [number, Uint8Array]);
		}));

		const blocks = blockBatches.flat();
		const outBlocks = new Array<Uint8Array>(positions.length);
		for (let i = 0; i < blocks.length; i++) {
			const [originalIndex, block] = blocks[i];
			outBlocks[originalIndex] = block;
		}
		if (outBlocks.some(block => !block))
			throw new Error("Missing block");
		return outBlocks;
	}
}

class ZstBlock {
	rows: Uint8Array[] = [];

	constructor(rows: Uint8Array[]) {
		this.rows = rows;
	}

	static async readRowAt(file: fsp.FileHandle, position: RowPosition): Promise<Uint8Array> {
		const compressedSize = await readUint32(file, position.blockOffset);
		console.log("compressedSize:", compressedSize);
		const compressedData = await readBytes(file, position.blockOffset + 4, compressedSize);
		const decompressedDataAr = await zstDecompress(compressedData);
		//const decompressedData = Buffer.from(decompressedDataAr.buffer);
		//const decompressedData = Uint8Array.from(decompressedDataAr);
		//const decompressedData = decompressedDataAr;

  const buffer = decompressedDataAr.buffer;
  console.log(">>>>", buffer)
  //console.log(new DataView(buffer).getUint32(0, true));
		//const rowCount = decompressedData.readUInt32LE(0);
		const rowCount = new DataView(buffer).getUint32(0, true);
    console.log(rowCount)
		const rowInfos: ZstRowInfo[] = new Array(rowCount);
		for (let i = 0; i < rowCount; i++) {
//  const uint32ReadBuffer = new Uint8Array(4);
//	//await file.read(uint32ReadBuffer, 0, 4, offset);
//  await Deno.seek(file.rid, offset, Deno.SeekMode.Start);
//	await Deno.read(file.rid, uint32ReadBuffer);
//  console.log(uint32ReadBuffer.getUint32(0, true))



			const rowInfo = ZstRowInfo.read(decompressedDataAr, 4 + i * ZstRowInfo.structSize);
			rowInfos[i] = rowInfo;
		}

		const dataStart = 4 + rowCount * ZstRowInfo.structSize;
		const rowInfo = rowInfos[position.rowIndex];
		return decompressedDataAr.subarray(dataStart + rowInfo.offset, dataStart + rowInfo.offset + rowInfo.size);
	}

	static async readMultipleRowsAt(file: fsp.FileHandle, blockOffset: number, positions: RowPosition[]): Promise<Uint8Array[]> {
		const compressedSize = await readUint32(file, blockOffset);
		const compressedData = await readBytes(file, blockOffset + 4, compressedSize);
		const decompressedData = await zstDecompress(compressedData);

		const rowCount = decompressedData.readUInt32LE(0);
		const rowInfos: ZstRowInfo[] = new Array(rowCount);
		for (let i = 0; i < rowCount; i++) {
			const rowInfo = ZstRowInfo.read(decompressedData, 4 + i * ZstRowInfo.structSize);
			rowInfos[i] = rowInfo;
		}

		const dataStart = 4 + rowCount * ZstRowInfo.structSize;
		const rows: Uint8Array[] = [];
		for (let i = 0; i < positions.length; i++) {
			const position = positions[i];
			const rowInfo = rowInfos[position.rowIndex];
			rows.push(decompressedData.subarray(dataStart + rowInfo.offset, dataStart + rowInfo.offset + rowInfo.size));
		}
		return rows;
	}
}

class ZstRowInfo {
	static readonly structSize = 8;
	offset: number;
	size: number;

	constructor(offset: number, size: number) {
		this.offset = offset;
		this.size = size;
	}

	static read(buffer: Buffer, offset: number): ZstRowInfo {
//  const buffer = uint32ReadBuffer.buffer;
//  console.log(new DataView(buffer).getUint32(0, true));
//	return uint32ReadBuffer;
//		const rowOffset = buffer.readUInt32LE(offset);
//		const rowSize = buffer.readUInt32LE(offset + 4);
		const rowOffset = new DataView(buffer.buffer).getUint32(offset, true);
		const rowSize = new DataView(buffer.buffer).getUint32(offset + 4, true)
		return new ZstRowInfo(rowOffset, rowSize);
	}

	write(buffer: Buffer, offset: number): void {
		buffer.writeUInt32LE(this.offset, offset);
		buffer.writeUInt32LE(this.size, offset + 4);
	}

}

export interface RowPosition {
	blockOffset: number;
	rowIndex: number;
}
export interface RowPositionWithIndex extends RowPosition {
	originalIndex: number;
}

async function readUint32(file: fsp.FileHandle, offset: number): Promise<number> {
//	const uint32ReadBuffer = Buffer.alloc(4);
  const uint32ReadBuffer = new Uint8Array(4);
	//await file.read(uint32ReadBuffer, 0, 4, offset);
  await Deno.seek(file.rid, offset, Deno.SeekMode.Start);
	//await Deno.read(uint32ReadBuffer, 0, 4, offset);
	await Deno.read(file.rid, uint32ReadBuffer);
//	return uint32ReadBuffer.readUInt32LE();
//  console.log(uint32ReadBuffer.getUint32(0, true))
  const buffer = uint32ReadBuffer.buffer;
  console.log(new DataView(buffer).getUint32(0, true));
//	return uint32ReadBuffer;
	return new DataView(buffer).getUint32(0, true);
}

async function readBytes(file: fsp.FileHandle, offset: number, length: number): Promise<Buffer> {
	//const bytes = Buffer.alloc(length);
	const bytes = new Uint8Array(length);
  await Deno.seek(file.rid, offset, Deno.SeekMode.Start);
  await Deno.read(file.rid, bytes);
//	await file.read(bytes, 0, length, offset);
  console.log(bytes)
	return bytes;
	//return new DataView(bytes.buffer).getUint32(0, true);;
}
