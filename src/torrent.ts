import fs, { promises as fsPromises } from "fs";
import fetch, { Response } from "node-fetch";
import path, { join } from "path";
import { inspect } from "util";
import {
	DATA_EXTENSIONS,
	EP_REGEX,
	GROUP_REGEX,
	MOVIE_REGEX,
	SEASON_REGEX,
	USER_AGENT,
	VIDEO_EXTENSIONS,
} from "./constants.js";
import { db } from "./db.js";
import { CrossSeedError } from "./errors.js";
import { Label, logger, logOnce } from "./logger.js";
import { Metafile } from "./parseTorrent.js";
import { Result, resultOf, resultOfErr } from "./Result.js";
import { getRuntimeConfig } from "./runtimeConfig.js";
import { createSearcheeFromTorrentFile, Searchee } from "./searchee.js";
import { stripExtension } from "./utils.js";
import { closest, distance } from "fastest-levenshtein";
import { getFileConfig } from "./configuration.js";

export interface TorrentLocator {
	infoHash?: string;
	name?: string;
	path?: string;
}

export enum SnatchError {
	ABORTED = "ABORTED",
	RATE_LIMITED = "RATE_LIMITED",
	MAGNET_LINK = "MAGNET_LINK",
	INVALID_CONTENTS = "INVALID_CONTENTS",
	UNKNOWN_ERROR = "UNKNOWN_ERROR",
}

export async function parseTorrentFromFilename(
	filename: string
): Promise<Metafile> {
	const data = await fsPromises.readFile(filename);
	return Metafile.decode(data);
}

export async function parseTorrentFromURL(
	url: string
): Promise<Result<Metafile, SnatchError>> {
	const abortController = new AbortController();
	const { snatchTimeout } = getRuntimeConfig();

	if (typeof snatchTimeout === "number") {
		setTimeout(() => void abortController.abort(), snatchTimeout).unref();
	}

	let response: Response;
	try {
		response = await fetch(url, {
			headers: { "User-Agent": USER_AGENT },
			signal: abortController.signal,
			redirect: "manual",
		});
	} catch (e) {
		if (e.name === "AbortError") {
			logger.error(`snatching ${url} timed out`);
			return resultOfErr(SnatchError.ABORTED);
		}
		logger.error(`failed to access ${url}`);
		logger.debug(e);
		return resultOfErr(SnatchError.UNKNOWN_ERROR);
	}

	if (
		response.status.toString().startsWith("3") &&
		response.headers.get("location")?.startsWith("magnet:")
	) {
		logger.error(`Unsupported: magnet link detected at ${url}`);
		return resultOfErr(SnatchError.MAGNET_LINK);
	} else if (response.status === 429) {
		return resultOfErr(SnatchError.RATE_LIMITED);
	} else if (!response.ok) {
		logger.error(
			`error downloading torrent at ${url}: ${response.status} ${response.statusText}`
		);
		logger.debug("response: %s", await response.text());
		return resultOfErr(SnatchError.UNKNOWN_ERROR);
	} else if (response.headers.get("Content-Type") === "application/rss+xml") {
		const responseText = await response.clone().text();
		if (responseText.includes("429")) {
			return resultOfErr(SnatchError.RATE_LIMITED);
		}
		logger.error(`invalid torrent contents at ${url}`);
		logger.debug(
			`contents: "${responseText.slice(0, 100)}${
				responseText.length > 100 ? "..." : ""
			}"`
		);
		return resultOfErr(SnatchError.INVALID_CONTENTS);
	}
	try {
		return resultOf(
			Metafile.decode(
				Buffer.from(new Uint8Array(await response.arrayBuffer()))
			)
		);
	} catch (e) {
		logger.error(`invalid torrent contents at ${url}`);
		const contentType = response.headers.get("Content-Type");
		const contentLength = response.headers.get("Content-Length");
		logger.debug(`Content-Type: ${contentType}`);
		logger.debug(`Content-Length: ${contentLength}`);
		return resultOfErr(SnatchError.INVALID_CONTENTS);
	}
}

export function saveTorrentFile(
	tracker: string,
	tag = "",
	meta: Metafile
): void {
	const { outputDir } = getRuntimeConfig();
	const buf = meta.encode();
	const filename = `[${tag}][${tracker}]${stripExtension(
		meta.getFileSystemSafeName()
	)}.torrent`;
	fs.writeFileSync(path.join(outputDir, filename), buf, { mode: 0o644 });
}

export async function findAllTorrentFilesInDir(
	torrentDir: string
): Promise<string[]> {
	return (await fsPromises.readdir(torrentDir))
		.filter((fn) => path.extname(fn) === ".torrent")
		.sort()
		.map((fn) => path.resolve(path.join(torrentDir, fn)));
}

export async function indexNewTorrents(): Promise<void> {
	const { torrentDir } = getRuntimeConfig();
	const dirContents = await findAllTorrentFilesInDir(torrentDir);
	// index new torrents in the torrentDir

	for (const filepath of dirContents) {
		const doesAlreadyExist = await db("torrent")
			.select("id")
			.where({ file_path: filepath })
			.first();

		if (!doesAlreadyExist) {
			let meta;
			try {
				meta = await parseTorrentFromFilename(filepath);
			} catch (e) {
				logOnce(`Failed to parse ${filepath}`, () => {
					logger.error(`Failed to parse ${filepath}`);
					logger.debug(e);
				});
				continue;
			}
			await db("torrent")
				.insert({
					file_path: filepath,
					info_hash: meta.infoHash,
					name: meta.name,
				})
				.onConflict("file_path")
				.ignore();
		}
	}
	// clean up torrents that no longer exist in the torrentDir
	// this might be a slow query
	await db("torrent").whereNotIn("file_path", dirContents).del();
}

export async function getInfoHashesToExclude(): Promise<string[]> {
	return (await db("torrent").select({ infoHash: "info_hash" })).map(
		(t) => t.infoHash
	);
}

export async function validateTorrentDir(): Promise<void> {
	const { torrentDir } = getRuntimeConfig();
	try {
		await fsPromises.readdir(torrentDir);
	} catch (e) {
		throw new CrossSeedError(`Torrent dir ${torrentDir} is invalid`);
	}
}

export async function loadTorrentDirLight(): Promise<Searchee[]> {
	const { torrentDir } = getRuntimeConfig();
	const torrentFilePaths = fs
		.readdirSync(torrentDir)
		.filter((fn) => path.extname(fn) === ".torrent")
		.sort()
		.map((filename) => join(getRuntimeConfig().torrentDir, filename));

	const searchees: Searchee[] = [];
	for (const torrentFilePath of torrentFilePaths) {
		const searcheeResult = await createSearcheeFromTorrentFile(
			torrentFilePath
		);
		if (searcheeResult.isOk()) {
			searchees.push(searcheeResult.unwrapOrThrow());
		}
	}
	return searchees;
}

const tokenizeName = (tname) =>
	tname
		.split(/[.\s-]+/)
		.map((x) => x.toLowerCase())
		.sort();
const createComparableTorrent = (x) => tokenizeName(x).join("");

function removeExtensionsFromName(name: string) {
	const extensionsToRemove = [...VIDEO_EXTENSIONS, ...DATA_EXTENSIONS];
	extensionsToRemove.forEach((extension) => {
		if (name.endsWith(extension)) {
			return name.endsWith(extension)
				? name.slice(0, -1 * extension.length)
				: name;
		}
	});
	return name;
}

export async function getTorrentByFuzzyName(
	name: string
): Promise<null | Metafile> {
	const searchTarget = createComparableTorrent(name);
	const searchTerm = EP_REGEX.test(name)
		? GROUP_REGEX.test(name)
			? `${name.match(EP_REGEX)[0]}%${name.match(GROUP_REGEX)[0]}`
			: name.match(EP_REGEX)[0]
		: SEASON_REGEX.test(name)
		? GROUP_REGEX.test(name)
			? `${name.match(SEASON_REGEX)[0]}%${name.match(GROUP_REGEX)[0]}`
			: name.match(SEASON_REGEX)[0]
		: GROUP_REGEX.test(name)
		? `${name.match(MOVIE_REGEX)[0]}%${name.match(GROUP_REGEX)[0]}`
		: name.match(MOVIE_REGEX)[0];

	const allTorrentNames = await db("torrent")
		.select("name", "file_path")
		.where("name", "LIKE", searchTerm);

	const searchMap = {};

	allTorrentNames.forEach((x) => {
		searchMap[createComparableTorrent(removeExtensionsFromName(x.name))] =
			x;
	});

	const closestMatch = closest(searchTarget, Object.keys(searchMap));
	const calcDistanceFrom = distance(searchTarget, closestMatch);

	const dissimilarityPct = calcDistanceFrom / searchTarget.length;
	const similarityPct = 100 * (1 - dissimilarityPct);
	const distanceMax = Math.max(
		(await getFileConfig()).levenshtein,
		Math.min(0.1 * searchTarget.length, 8)
	);

	if (distanceMax >= calcDistanceFrom - 2) {
		logger.verbose({
			label: Label.DECIDE,
			message: `[levenshtein(${distanceMax})] -> ${searchMap[closestMatch].name}\n\t\t -> ${name}\n\t\t\t = ${similarityPct}% (${calcDistanceFrom})`,
		});
		if (distanceMax >= calcDistanceFrom) {
			return parseTorrentFromFilename(searchMap[closestMatch].file_path);
		}
	}
	return null;
}

export async function getTorrentByCriteria(
	criteria: TorrentLocator
): Promise<Metafile> {
	const findResult = await db("torrent")
		.where((b) => {
			// there is always at least one criterion
			if (criteria.infoHash) {
				b = b.where({ info_hash: criteria.infoHash });
			}
			if (criteria.name) {
				b = b.where({ name: criteria.name });
			}
			return b;
		})
		.first();

	if (findResult === undefined) {
		const message = `could not find a torrent with the criteria ${inspect(
			criteria
		)}`;
		throw new Error(message);
	}
	return parseTorrentFromFilename(findResult.file_path);
}
