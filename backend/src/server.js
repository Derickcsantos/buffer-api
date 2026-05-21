import { createServer } from "node:http";
import { createReadStream, createWriteStream, readFileSync } from "node:fs";
import { mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, extname, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const ROOT_DIR = resolve(__dirname, "../..");
const FRONTEND_DIR = resolve(ROOT_DIR, "frontend");

loadEnv(resolve(ROOT_DIR, ".env"));

const config = {
  port: Number(process.env.PORT || 3333),
  bufferApiUrl: process.env.BUFFER_API_URL || "https://api.buffer.com",
  bufferApiKey: process.env.BUFFER_API_KEY || "",
  organizationId: process.env.BUFFER_ORGANIZATION_ID || "",
  channelId: process.env.BUFFER_CHANNEL_ID || "",
  databaseUrl: process.env.DATABASE_URL || "",
  datasetFile: process.env.MEDIA_DATASET_FILE || "dataset_instagram-scraper_2026-05-21_15-30-00-859.json",
  outputDir: process.env.MEDIA_OUTPUT_DIR || "media-export",
  publicMediaBaseUrl: process.env.PUBLIC_MEDIA_BASE_URL || "",
  bufferMediaSource: process.env.BUFFER_MEDIA_SOURCE || "auto",
  bufferPostType: process.env.BUFFER_POST_TYPE || "post",
  bufferRequestDelayMs: Number(process.env.BUFFER_REQUEST_DELAY_MS || 750),
  bufferScheduleTimes: (process.env.BUFFER_SCHEDULE_TIMES || "09:00,13:00,18:00")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
};

const database = config.databaseUrl
  ? new pg.Pool({
      connectionString: config.databaseUrl,
      ssl: { rejectUnauthorized: false }
    })
  : null;

const mediaMimeTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".zip", "application/zip"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"],
  [".png", "image/png"],
  [".webp", "image/webp"],
  [".gif", "image/gif"],
  [".mp4", "video/mp4"],
  [".mov", "video/quicktime"]
]);

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url || "/", `http://${request.headers.host}`);

    if (url.pathname === "/api/health" && request.method === "GET") {
      return sendJson(response, 200, { ok: true });
    }

    if (url.pathname === "/api/media/export" && request.method === "POST") {
      const body = await readJson(request).catch(() => ({}));
      const result = await exportMedia({ force: Boolean(body.force) });
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/media/zip" && request.method === "GET") {
      return sendFile(response, resolve(ROOT_DIR, config.outputDir, "media-export.zip"), "media-export.zip");
    }

    if (url.pathname.startsWith(`/${config.outputDir}/`) && request.method === "GET") {
      const relativeMediaPath = decodeURIComponent(url.pathname.slice(config.outputDir.length + 2));
      const filePath = resolve(ROOT_DIR, config.outputDir, relativeMediaPath);
      const mediaRoot = resolve(ROOT_DIR, config.outputDir);
      if (!filePath.startsWith(mediaRoot)) return sendJson(response, 403, { error: "Acesso negado" });
      return sendFile(response, filePath);
    }

    if (url.pathname === "/api/database/import" && request.method === "POST") {
      const result = await importDatasetToDatabase();
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/database/status" && request.method === "GET") {
      const result = await getDatabaseStatus();
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/buffer/posts" && request.method === "POST") {
      const body = await readJson(request);
      const result = await createBufferPost(body);
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/buffer/publish-all" && request.method === "POST") {
      const body = await readJson(request).catch(() => ({}));
      const result = await publishDatabasePostsToBuffer({
        mode: "addToQueue",
        force: Boolean(body.force),
        limit: optionalPositiveInteger(body.limit),
        channelId: body.channelId || config.channelId
      });
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/buffer/schedule-all" && request.method === "POST") {
      const body = await readJson(request).catch(() => ({}));
      const result = await publishDatabasePostsToBuffer({
        mode: "customScheduled",
        force: Boolean(body.force),
        limit: optionalPositiveInteger(body.limit),
        channelId: body.channelId || config.channelId,
        postsPerDay: optionalPositiveInteger(body.postsPerDay) || 3,
        startDate: body.startDate || null,
        times: Array.isArray(body.times) && body.times.length ? body.times : config.bufferScheduleTimes
      });
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/buffer/channels" && request.method === "GET") {
      const result = await getBufferChannels(url.searchParams.get("organizationId") || config.organizationId);
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/buffer/posts" && request.method === "GET") {
      const result = await getBufferPosts({
        after: url.searchParams.get("after") || "",
        first: Number(url.searchParams.get("first") || 20),
        status: url.searchParams.get("status") || "sent",
        channelId: url.searchParams.get("channelId") || config.channelId,
        organizationId: url.searchParams.get("organizationId") || config.organizationId
      });
      return sendJson(response, 200, result);
    }

    if (url.pathname === "/api/config" && request.method === "GET") {
      return sendJson(response, 200, {
        hasBufferApiKey: Boolean(config.bufferApiKey),
        hasDatabaseUrl: Boolean(config.databaseUrl),
        channelId: config.channelId,
        organizationId: config.organizationId,
        datasetFile: config.datasetFile,
        outputDir: config.outputDir,
        bufferScheduleTimes: config.bufferScheduleTimes,
        bufferRequestDelayMs: config.bufferRequestDelayMs
      });
    }

    return serveFrontend(request, response, url);
  } catch (error) {
    console.error(error);
    return sendJson(response, error.statusCode || 500, {
      error: error.message || "Erro interno"
    });
  }
});

await initializeDatabase();

server.listen(config.port, () => {
  console.log(`Buffer manager rodando em http://localhost:${config.port}`);
});

async function exportMedia({ force = false } = {}) {
  const datasetPath = resolve(ROOT_DIR, config.datasetFile);
  const outputDir = resolve(ROOT_DIR, config.outputDir);
  const filesDir = resolve(outputDir, "files");
  const zipPath = resolve(outputDir, "media-export.zip");
  const manifestPath = resolve(outputDir, "media-manifest.json");

  logExport(`Iniciando exportacao. Dataset: ${datasetPath}`);
  logExport(`Pasta de saida: ${outputDir}`);
  logExport(`Forcar novo download: ${force ? "sim" : "nao"}`);

  await mkdir(filesDir, { recursive: true });

  const posts = JSON.parse(await readFile(datasetPath, "utf8"));
  const records = collectMediaRecords(posts);
  const downloaded = [];
  const failed = [];

  logExport(`Posts lidos: ${Array.isArray(posts) ? posts.length : 0}`);
  logExport(`Midias encontradas: ${records.length}`);

  for (const [index, record] of records.entries()) {
    const extension = extensionFromUrl(record.url, record.kind);
    const fileName = `${String(index + 1).padStart(5, "0")}-${record.postId}-${record.kind}${extension}`;
    const filePath = resolve(filesDir, sanitizeFileName(fileName));
    const relativePath = toPosixPath(filePath.slice(outputDir.length + 1));
    const progress = `${index + 1}/${records.length}`;

    try {
      if (!force && await exists(filePath)) {
        logExport(`[${progress}] Pulando arquivo existente: ${relativePath}`);
        downloaded.push({ ...record, fileName: basename(filePath), path: relativePath, skipped: true });
        continue;
      }

      logExport(`[${progress}] Baixando ${record.kind} do post ${record.shortCode || record.postId}`);
      await downloadFileWithRetry(record.url, filePath, { attempts: 3 });
      logExport(`[${progress}] Salvo em ${relativePath}`);
      downloaded.push({ ...record, fileName: basename(filePath), path: relativePath, skipped: false });
    } catch (error) {
      logExport(`[${progress}] Falha ao baixar ${record.url}: ${error.message}`, "error");
      failed.push({ ...record, error: error.message });
    }
  }

  const manifest = {
    exportedAt: new Date().toISOString(),
    datasetFile: config.datasetFile,
    total: records.length,
    downloaded: downloaded.length,
    failed: failed.length,
    files: downloaded.map(({ postId, shortCode, type, kind, source, url, fileName, path, skipped }) => ({
      postId,
      shortCode,
      type,
      kind,
      source,
      url,
      fileName,
      path,
      skipped
    }))
  };

  await writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  logExport(`Manifesto salvo em ${manifestPath}`);
  logExport(`Criando ZIP em ${zipPath}`);
  await createZipFromDirectory(filesDir, zipPath);
  logExport(`ZIP criado. Baixadas/preservadas: ${downloaded.length}. Falhas: ${failed.length}.`);

  return {
    ...manifest,
    outputDir: config.outputDir,
    zip: `${config.outputDir}/media-export.zip`,
    downloadUrl: "/api/media/zip",
    failed
  };
}

function logExport(message, level = "log") {
  const timestamp = new Date().toISOString();
  const line = `[media-export ${timestamp}] ${message}`;
  if (level === "error") console.error(line);
  else console.log(line);
}

async function initializeDatabase() {
  if (!database) {
    console.warn("[database] DATABASE_URL nao configurado. Rotas de banco ficarao indisponiveis.");
    return;
  }

  await database.query(`
    CREATE TABLE IF NOT EXISTS instagram_posts (
      id TEXT PRIMARY KEY,
      shortcode TEXT UNIQUE,
      title TEXT NOT NULL,
      description TEXT,
      caption TEXT,
      post_type TEXT,
      instagram_url TEXT,
      owner_username TEXT,
      owner_full_name TEXT,
      owner_id TEXT,
      hashtags TEXT[] NOT NULL DEFAULT '{}',
      mentions TEXT[] NOT NULL DEFAULT '{}',
      published_at TIMESTAMPTZ,
      dimensions_width INTEGER,
      dimensions_height INTEGER,
      media_count INTEGER NOT NULL DEFAULT 0,
      raw JSONB NOT NULL DEFAULT '{}'::jsonb,
      imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS instagram_post_media (
      id BIGSERIAL PRIMARY KEY,
      post_id TEXT NOT NULL REFERENCES instagram_posts(id) ON DELETE CASCADE,
      media_url TEXT NOT NULL,
      media_type TEXT NOT NULL,
      source TEXT,
      position INTEGER NOT NULL DEFAULT 0,
      shortcode TEXT,
      dimensions_width INTEGER,
      dimensions_height INTEGER,
      alt_text TEXT,
      local_path TEXT,
      local_file_name TEXT,
      raw JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (post_id, media_url)
    );

    ALTER TABLE instagram_post_media
      ADD COLUMN IF NOT EXISTS local_path TEXT,
      ADD COLUMN IF NOT EXISTS local_file_name TEXT;

    CREATE INDEX IF NOT EXISTS idx_instagram_posts_published_at ON instagram_posts (published_at);
    CREATE INDEX IF NOT EXISTS idx_instagram_posts_post_type ON instagram_posts (post_type);
    CREATE INDEX IF NOT EXISTS idx_instagram_post_media_post_id ON instagram_post_media (post_id);
    CREATE INDEX IF NOT EXISTS idx_instagram_post_media_media_type ON instagram_post_media (media_type);

    CREATE TABLE IF NOT EXISTS buffer_post_exports (
      id BIGSERIAL PRIMARY KEY,
      instagram_post_id TEXT NOT NULL REFERENCES instagram_posts(id) ON DELETE CASCADE,
      buffer_post_id TEXT,
      channel_id TEXT NOT NULL,
      mode TEXT NOT NULL,
      due_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'pending',
      error_message TEXT,
      response JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    DROP INDEX IF EXISTS idx_buffer_post_exports_unique_success;

    CREATE UNIQUE INDEX IF NOT EXISTS idx_buffer_post_exports_unique_latest
      ON buffer_post_exports (instagram_post_id, channel_id, mode);

    CREATE INDEX IF NOT EXISTS idx_buffer_post_exports_instagram_post_id
      ON buffer_post_exports (instagram_post_id);
    CREATE INDEX IF NOT EXISTS idx_buffer_post_exports_status
      ON buffer_post_exports (status);
  `);

  console.log("[database] Schema verificado/criado com sucesso.");
}

async function importDatasetToDatabase() {
  ensureDatabase();

  const datasetPath = resolve(ROOT_DIR, config.datasetFile);
  logDatabase(`Importando dataset: ${datasetPath}`);

  const posts = JSON.parse(await readFile(datasetPath, "utf8"));
  const normalizedPosts = (Array.isArray(posts) ? posts : []).map(normalizePostForDatabase);
  const mediaRows = normalizedPosts.flatMap((post) => post.media.map((media) => ({ post, media })));
  const client = await database.connect();

  try {
    await client.query("BEGIN");

    for (const [index, chunk] of chunks(normalizedPosts, 100).entries()) {
      logDatabase(`Salvando lote de posts ${index + 1}: ${chunk.length} registros`);
      await upsertPostChunk(client, chunk);
    }

    if (normalizedPosts.length) {
      logDatabase("Limpando midias antigas dos posts importados");
      await client.query("DELETE FROM instagram_post_media WHERE post_id = ANY($1::text[])", [
        normalizedPosts.map((post) => post.id)
      ]);
    }

    for (const [index, chunk] of chunks(mediaRows, 250).entries()) {
      logDatabase(`Salvando lote de midias ${index + 1}: ${chunk.length} registros`);
      await insertMediaChunk(client, chunk);
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  logDatabase(`Importacao concluida. Posts: ${normalizedPosts.length}. Midias: ${mediaRows.length}.`);

  return {
    importedAt: new Date().toISOString(),
    datasetFile: config.datasetFile,
    posts: normalizedPosts.length,
    media: mediaRows.length,
    tables: ["instagram_posts", "instagram_post_media"]
  };
}

async function upsertPostChunk(client, posts) {
  const columnsPerRow = 17;
  const values = [];
  const placeholders = posts.map((post, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
    values.push(
      post.id,
      post.shortcode,
      post.title,
      post.description,
      post.caption,
      post.postType,
      post.instagramUrl,
      post.ownerUsername,
      post.ownerFullName,
      post.ownerId,
      post.hashtags,
      post.mentions,
      post.publishedAt,
      post.dimensionsWidth,
      post.dimensionsHeight,
      post.media.length,
      JSON.stringify(post.raw)
    );
    return `(
      $${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7},
      $${offset + 8}, $${offset + 9}, $${offset + 10}, $${offset + 11}, $${offset + 12},
      $${offset + 13}, $${offset + 14}, $${offset + 15}, $${offset + 16}, $${offset + 17}::jsonb, now()
    )`;
  });

  await client.query(
    `
      INSERT INTO instagram_posts (
        id, shortcode, title, description, caption, post_type, instagram_url,
        owner_username, owner_full_name, owner_id, hashtags, mentions,
        published_at, dimensions_width, dimensions_height, media_count, raw, updated_at
      )
      VALUES ${placeholders.join(",")}
      ON CONFLICT (id) DO UPDATE SET
        shortcode = EXCLUDED.shortcode,
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        caption = EXCLUDED.caption,
        post_type = EXCLUDED.post_type,
        instagram_url = EXCLUDED.instagram_url,
        owner_username = EXCLUDED.owner_username,
        owner_full_name = EXCLUDED.owner_full_name,
        owner_id = EXCLUDED.owner_id,
        hashtags = EXCLUDED.hashtags,
        mentions = EXCLUDED.mentions,
        published_at = EXCLUDED.published_at,
        dimensions_width = EXCLUDED.dimensions_width,
        dimensions_height = EXCLUDED.dimensions_height,
        media_count = EXCLUDED.media_count,
        raw = EXCLUDED.raw,
        updated_at = now()
    `,
    values
  );
}

async function insertMediaChunk(client, rows) {
  if (!rows.length) return;

  const columnsPerRow = 12;
  const values = [];
  const placeholders = rows.map(({ post, media }, rowIndex) => {
    const offset = rowIndex * columnsPerRow;
    values.push(
      post.id,
      media.url,
      media.type,
      media.source,
      media.position,
      media.shortcode,
      media.dimensionsWidth,
      media.dimensionsHeight,
      media.alt,
      media.localPath,
      media.localFileName,
      JSON.stringify(media.raw)
    );
    return `(
      $${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5},
      $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10},
      $${offset + 11}, $${offset + 12}::jsonb
    )`;
  });

  await client.query(
    `
      INSERT INTO instagram_post_media (
        post_id, media_url, media_type, source, position, shortcode,
        dimensions_width, dimensions_height, alt_text, local_path, local_file_name, raw
      )
      VALUES ${placeholders.join(",")}
      ON CONFLICT (post_id, media_url) DO NOTHING
    `,
    values
  );
}

async function getDatabaseStatus() {
  ensureDatabase();
  const result = await database.query(`
    SELECT
      (SELECT COUNT(*)::int FROM instagram_posts) AS posts,
      (SELECT COUNT(*)::int FROM instagram_post_media) AS media
  `);

  return {
    connected: true,
    posts: result.rows[0]?.posts || 0,
    media: result.rows[0]?.media || 0
  };
}

function normalizePostForDatabase(post) {
  const caption = cleanText(post.caption || "");
  const hashtags = uniqueStrings([
    ...normalizeStringArray(post.hashtags),
    ...extractHashtags(caption)
  ]);
  const mentions = uniqueStrings([
    ...normalizeStringArray(post.mentions),
    ...extractMentions(caption)
  ]);
  const media = collectPostMediaForDatabase(post);

  return {
    id: String(post.id || post.shortCode),
    shortcode: post.shortCode || null,
    title: titleFromPost(post, caption),
    description: caption || null,
    caption: caption || null,
    postType: post.type || null,
    instagramUrl: post.url || null,
    ownerUsername: post.ownerUsername || null,
    ownerFullName: post.ownerFullName || null,
    ownerId: post.ownerId ? String(post.ownerId) : null,
    hashtags,
    mentions,
    publishedAt: validDateOrNull(post.timestamp),
    dimensionsWidth: integerOrNull(post.dimensionsWidth),
    dimensionsHeight: integerOrNull(post.dimensionsHeight),
    media,
    raw: {
      id: post.id,
      shortCode: post.shortCode,
      type: post.type,
      inputUrl: post.inputUrl,
      isCommentsDisabled: post.isCommentsDisabled
    }
  };
}

function collectPostMediaForDatabase(post) {
  const media = [];
  const seen = new Set();

  const add = (url, type, source, sourcePost, positionHint = media.length) => {
    if (!url || typeof url !== "string" || seen.has(url)) return;
    seen.add(url);
    media.push({
      url,
      type: type === "video" ? "video" : "image",
      source,
      position: positionHint,
      shortcode: sourcePost.shortCode || post.shortCode || null,
      dimensionsWidth: integerOrNull(sourcePost.dimensionsWidth || post.dimensionsWidth),
      dimensionsHeight: integerOrNull(sourcePost.dimensionsHeight || post.dimensionsHeight),
      alt: sourcePost.alt || null,
      localPath: mediaManifestByUrl().get(url)?.path || null,
      localFileName: mediaManifestByUrl().get(url)?.fileName || null,
      raw: {
        postId: sourcePost.id || post.id,
        sourceType: sourcePost.type || post.type
      }
    });
  };

  for (const [index, image] of (Array.isArray(post.images) ? post.images : []).entries()) {
    add(image, "image", "images", post, index);
  }

  add(post.videoUrl || post.video_url || post.video || post.videoPlayUrl, "video", "videoUrl", post);
  add(post.displayUrl, inferKind(post.displayUrl, post.type), "displayUrl", post);

  for (const child of Array.isArray(post.childPosts) ? post.childPosts : []) {
    add(child.videoUrl || child.video_url || child.video || child.videoPlayUrl, "video", "childVideoUrl", child);
    add(child.displayUrl, inferKind(child.displayUrl, child.type), "childDisplayUrl", child);
    for (const [index, image] of (Array.isArray(child.images) ? child.images : []).entries()) {
      add(image, "image", "childImages", child, index);
    }
  }

  return media.map((item, index) => ({ ...item, position: index + 1 }));
}

function titleFromPost(post, caption) {
  const firstLine = caption.split(/\r?\n/).map((line) => line.trim()).find(Boolean);
  if (firstLine) return firstLine.slice(0, 160);
  if (post.shortCode) return `Post ${post.shortCode}`;
  return `Post ${post.id}`;
}

function extractHashtags(text) {
  return [...String(text).matchAll(/#([\p{L}\p{N}_]+)/gu)].map((match) => match[1]);
}

function extractMentions(text) {
  return [...String(text).matchAll(/@([\w.]+)/g)].map((match) => match[1]);
}

function normalizeStringArray(value) {
  return (Array.isArray(value) ? value : [])
    .map((item) => String(item).replace(/^[@#]/, "").trim())
    .filter(Boolean);
}

function uniqueStrings(values) {
  return [...new Set(values.map((item) => String(item).trim()).filter(Boolean))];
}

function chunks(items, size) {
  const result = [];
  for (let index = 0; index < items.length; index += size) {
    result.push(items.slice(index, index + size));
  }
  return result;
}

function cleanText(value) {
  return String(value || "").trim();
}

function validDateOrNull(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function integerOrNull(value) {
  const number = Number(value);
  return Number.isInteger(number) ? number : null;
}

function logDatabase(message) {
  console.log(`[database ${new Date().toISOString()}] ${message}`);
}

function collectMediaRecords(posts) {
  const records = [];
  const seen = new Set();

  const visit = (post, parentPost = null) => {
    if (!post || typeof post !== "object") return;

    const postId = String(post.id || parentPost?.id || "sem-id");
    const shortCode = post.shortCode || parentPost?.shortCode || "";
    const type = post.type || parentPost?.type || "Media";

    const add = (url, kind, source) => {
      if (!url || typeof url !== "string") return;
      const key = url;
      if (seen.has(key)) return;
      seen.add(key);
      records.push({ postId, shortCode, type, kind, source, url });
    };

    for (const image of Array.isArray(post.images) ? post.images : []) add(image, "image", "images");
    add(post.displayUrl, inferKind(post.displayUrl, post.type), "displayUrl");
    add(post.videoUrl || post.video_url || post.video || post.videoPlayUrl, "video", "videoUrl");

    for (const child of Array.isArray(post.childPosts) ? post.childPosts : []) visit(child, post);
  };

  for (const post of Array.isArray(posts) ? posts : []) visit(post);
  return records;
}

async function createBufferPost(body) {
  ensureBufferCredentials();

  const channelId = body.channelId || config.channelId;
  if (!channelId) throw badRequest("Informe channelId no corpo ou BUFFER_CHANNEL_ID no .env.");

  const assets = normalizeAssets(body.assets || [], body.mediaFiles || []);
  const mode = body.mode || (body.dueAt ? "customScheduled" : "addToQueue");
  const schedulingType = body.schedulingType || "automatic";
  const dueAtLine = mode === "customScheduled" ? `dueAt: ${graphqlString(required(body.dueAt, "dueAt"))}` : "";
  const metadataLine = `metadata: ${graphqlMetadata(body.metadata || {
    instagram: {
      type: body.type || config.bufferPostType,
      shouldShareToFeed: true
    }
  })}`;

  const query = `
    mutation CreatePost {
      createPost(input: {
        text: ${graphqlString(body.text || "")}
        channelId: ${graphqlString(channelId)}
        schedulingType: ${schedulingType}
        mode: ${mode}
        ${dueAtLine}
        ${metadataLine}
        ${assets.length ? `assets: ${graphqlAssets(assets)}` : ""}
      }) {
        ... on PostActionSuccess {
          post {
            id
            text
            createdAt
            channelId
            assets {
              id
              mimeType
              source
            }
          }
        }
        ... on MutationError {
          message
        }
      }
    }
  `;

  return bufferGraphql(query);
}

async function getBufferPosts({ after = "", first = 20, status = "sent", channelId, organizationId }) {
  ensureBufferCredentials();
  if (!organizationId) throw badRequest("Informe organizationId na query ou BUFFER_ORGANIZATION_ID no .env.");

  const statuses = status.split(",").map((item) => item.trim()).filter(Boolean).join(", ");
  const channelFilter = channelId ? `channelIds: [${graphqlString(channelId)}]` : "";
  const afterLine = after ? `after: ${graphqlString(after)}` : "";

  const query = `
    query GetPosts {
      posts(
        ${afterLine}
        first: ${Math.min(Math.max(first, 1), 100)}
        input: {
          organizationId: ${graphqlString(organizationId)}
          filter: {
            status: [${statuses || "sent"}]
            ${channelFilter}
          }
        }
      ) {
        pageInfo {
          startCursor
          endCursor
          hasNextPage
        }
        edges {
          node {
            id
            text
            createdAt
            channelId
          }
        }
      }
    }
  `;

  return bufferGraphql(query);
}

async function getBufferChannels(organizationId) {
  ensureBufferCredentials();
  if (!organizationId) throw badRequest("Informe organizationId na query ou BUFFER_ORGANIZATION_ID no .env.");

  const query = `
    query GetChannels {
      channels(input: {
        organizationId: ${graphqlString(organizationId)}
      }) {
        id
        name
        displayName
        service
        avatar
        isQueuePaused
      }
    }
  `;

  return bufferGraphql(query);
}

async function publishDatabasePostsToBuffer({
  mode,
  force = false,
  limit = null,
  channelId,
  postsPerDay = 3,
  startDate = null,
  times = config.bufferScheduleTimes
}) {
  ensureDatabase();
  ensureBufferCredentials();
  if (!channelId) throw badRequest("Informe channelId no corpo ou BUFFER_CHANNEL_ID no .env.");
  ensureUsableMediaSource();

  const posts = await getDatabasePostsForBuffer({ channelId, mode, force, limit });
  const previouslyExported = force ? 0 : await countSuccessfulBufferExports({ channelId, mode });
  const results = [];

  logBufferBatch(`Iniciando envio para Buffer. Modo: ${mode}. Posts: ${posts.length}. Force: ${force ? "sim" : "nao"}.`);

  for (const [index, post] of posts.entries()) {
    const dueAt = mode === "customScheduled"
      ? scheduledDateForIndex(index, { startDate, postsPerDay, times })
      : null;
    const progress = `${index + 1}/${posts.length}`;

    try {
      logBufferBatch(`[${progress}] Enviando ${post.shortcode || post.id}${dueAt ? ` para ${dueAt}` : ""}`);
      const assets = await bufferAssetsFromPost(post);
      if (!assets.length) throw new Error("Post sem midia utilizavel para o Buffer.");

      const payload = {
        text: postTextForBuffer(post),
        channelId,
        mode,
        dueAt,
        type: config.bufferPostType,
        assets
      };
      const assetSources = [...new Set(payload.assets.map((asset) => asset.source))].join(", ");
      logBufferBatch(`[${progress}] Assets: ${payload.assets.length} (${assetSources || "sem midia"})`);
      const response = await createBufferPost(payload);
      const createdPost = response?.data?.createPost?.post;
      const mutationMessage = response?.data?.createPost?.message;
      const graphQlMessage = response?.errors?.map((error) => error.message).join("; ");

      if (!createdPost?.id) {
        throw new Error(mutationMessage || graphQlMessage || "Buffer nao retornou id do post criado.");
      }

      await saveBufferExportResult({
        instagramPostId: post.id,
        bufferPostId: createdPost.id,
        channelId,
        mode,
        dueAt,
        status: "success",
        response
      });

      results.push({
        instagramPostId: post.id,
        shortcode: post.shortcode,
        bufferPostId: createdPost.id,
        dueAt,
        status: "success"
      });
    } catch (error) {
      logBufferBatch(`[${progress}] Falha no post ${post.shortcode || post.id}: ${error.message}`, "error");
      await saveBufferExportResult({
        instagramPostId: post.id,
        channelId,
        mode,
        dueAt,
        status: "error",
        errorMessage: error.message,
        response: error.response || {}
      });

      results.push({
        instagramPostId: post.id,
        shortcode: post.shortcode,
        dueAt,
        status: "error",
        error: error.message
      });
    }

    if (config.bufferRequestDelayMs > 0 && index < posts.length - 1) {
      await sleep(config.bufferRequestDelayMs);
    }
  }

  const summary = {
    mode,
    channelId,
    total: results.length,
    success: results.filter((item) => item.status === "success").length,
    failed: results.filter((item) => item.status === "error").length,
    skippedPreviouslyExported: previouslyExported,
    results
  };

  logBufferBatch(`Envio concluido. Sucesso: ${summary.success}. Falhas: ${summary.failed}.`);
  return summary;
}

function ensureUsableMediaSource() {
  if (config.bufferMediaSource === "local" && !config.publicMediaBaseUrl) {
    throw badRequest("BUFFER_MEDIA_SOURCE=local exige PUBLIC_MEDIA_BASE_URL apontando para a pasta media-export publica.");
  }
}

async function getDatabasePostsForBuffer({ channelId, mode, force, limit }) {
  const result = await database.query(
    `
      SELECT
        p.id,
        p.shortcode,
        p.title,
        p.description,
        p.caption,
        p.published_at,
        COALESCE(
          jsonb_agg(
            jsonb_build_object(
              'mediaUrl', m.media_url,
              'mediaType', m.media_type,
              'position', m.position,
              'width', COALESCE(m.dimensions_width, p.dimensions_width),
              'height', COALESCE(m.dimensions_height, p.dimensions_height),
              'altText', COALESCE(m.alt_text, ''),
              'localPath', m.local_path,
              'localFileName', m.local_file_name
            )
            ORDER BY m.position
          ) FILTER (WHERE m.id IS NOT NULL),
          '[]'::jsonb
        ) AS media
      FROM instagram_posts p
      LEFT JOIN instagram_post_media m ON m.post_id = p.id
      WHERE (
        $1::boolean = true
        OR NOT EXISTS (
          SELECT 1
          FROM buffer_post_exports e
          WHERE e.instagram_post_id = p.id
            AND e.channel_id = $2
            AND e.mode = $3
            AND e.status = 'success'
        )
      )
      GROUP BY p.id
      HAVING COUNT(m.id) > 0
      ORDER BY p.published_at ASC NULLS LAST, p.id ASC
      ${limit ? `LIMIT ${limit}` : ""}
    `,
    [force, channelId, mode]
  );

  return result.rows.map((post) => ({
    ...post,
    media: normalizeDatabaseMedia(post.media)
  }));
}

async function saveBufferExportResult({
  instagramPostId,
  bufferPostId = null,
  channelId,
  mode,
  dueAt = null,
  status,
  errorMessage = null,
  response = {}
}) {
  await database.query(
    `
      INSERT INTO buffer_post_exports (
        instagram_post_id, buffer_post_id, channel_id, mode, due_at,
        status, error_message, response, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())
      ON CONFLICT (instagram_post_id, channel_id, mode) DO UPDATE SET
        buffer_post_id = EXCLUDED.buffer_post_id,
        due_at = EXCLUDED.due_at,
        status = EXCLUDED.status,
        error_message = EXCLUDED.error_message,
        response = EXCLUDED.response,
        updated_at = now()
    `,
    [
      instagramPostId,
      bufferPostId,
      channelId,
      mode,
      dueAt,
      status,
      errorMessage,
      JSON.stringify(response)
    ]
  );
}

async function countSuccessfulBufferExports({ channelId, mode }) {
  const result = await database.query(
    "SELECT COUNT(*)::int AS total FROM buffer_post_exports WHERE channel_id = $1 AND mode = $2 AND status = 'success'",
    [channelId, mode]
  );
  return result.rows[0]?.total || 0;
}

function normalizeAssets(assets, mediaFiles) {
  const normalized = [];

  for (const asset of assets) {
    if (asset.image?.url) normalized.push({ type: "image", url: asset.image.url });
    else if (asset.video?.url) normalized.push({ type: "video", url: asset.video.url });
    else if (asset.type && asset.url) normalized.push({ type: asset.type, url: asset.url });
  }

  for (const mediaFile of mediaFiles) {
    const file = typeof mediaFile === "string" ? mediaFile : mediaFile.path;
    const type = typeof mediaFile === "string" ? inferKind(mediaFile) : mediaFile.type || inferKind(file);
    if (!config.publicMediaBaseUrl) {
      throw badRequest("Configure PUBLIC_MEDIA_BASE_URL para criar posts com arquivos locais exportados.");
    }
    normalized.push({
      type,
      url: `${config.publicMediaBaseUrl.replace(/\/$/, "")}/${String(file).replace(/^\/+/, "")}`
    });
  }

  return normalized;
}

function graphqlAssets(assets) {
  const items = assets.map((asset) => {
    const type = asset.type === "video" ? "video" : "image";
    return `{ ${type}: ${graphqlAssetPayload(type, asset)} }`;
  });
  return `[${items.join(", ")}]`;
}

function graphqlAssetPayload(type, asset) {
  if (type === "video") {
    return `{
      url: ${graphqlString(asset.url)}
      ${asset.thumbnailUrl ? `thumbnailUrl: ${graphqlString(asset.thumbnailUrl)}` : ""}
      ${asset.title ? `metadata: { title: ${graphqlString(asset.title)} }` : ""}
    }`;
  }

  const width = integerOrNull(asset.width);
  const height = integerOrNull(asset.height);
  const metadata = width && height
    ? `metadata: {
        altText: ${graphqlString(asset.altText || "")}
        dimensions: {
          width: ${width}
          height: ${height}
        }
      }`
    : "";

  return `{
    url: ${graphqlString(asset.url)}
    ${asset.thumbnailUrl ? `thumbnailUrl: ${graphqlString(asset.thumbnailUrl)}` : ""}
    ${metadata}
  }`;
}

function graphqlMetadata(metadata) {
  if (metadata.instagram) {
    const instagram = metadata.instagram;
    return `{
      instagram: {
        type: ${instagram.type || config.bufferPostType}
        shouldShareToFeed: ${instagram.shouldShareToFeed !== false}
        ${instagram.firstComment ? `firstComment: ${graphqlString(instagram.firstComment)}` : ""}
        ${instagram.link ? `link: ${graphqlString(instagram.link)}` : ""}
      }
    }`;
  }

  return "{}";
}

function postTextForBuffer(post) {
  return cleanText(post.caption || post.description || post.title || "");
}

async function bufferAssetsFromPost(post) {
  const media = normalizeDatabaseMedia(post.media);
  const videos = media.filter((item) => item.mediaType === "video");
  const selectedMedia = videos.length ? videos : media.filter((item) => item.mediaType === "image");

  const assets = [];
  for (const item of selectedMedia) {
    const resolvedUrl = await resolveMediaUrlForBuffer(item);
    if (!resolvedUrl) {
      logBufferBatch(`Midia indisponivel, pulando: ${item.mediaUrl}`, "error");
      continue;
    }
    assets.push({
      type: item.mediaType === "video" ? "video" : "image",
      url: resolvedUrl.url,
      source: resolvedUrl.source,
      width: item.width,
      height: item.height,
      altText: item.altText || ""
    });
  }

  return assets;
}

async function resolveMediaUrlForBuffer(media) {
  const originalUrl = media.mediaUrl;
  if (config.bufferMediaSource === "json") {
    return await isRemoteUrlReachable(originalUrl) ? { url: originalUrl, source: "json" } : null;
  }

  const localPath = media.localPath || mediaManifestByUrl().get(originalUrl)?.path;
  if (localPath && config.publicMediaBaseUrl) {
    const localCandidate = {
      url: `${config.publicMediaBaseUrl.replace(/\/$/, "")}/${localPath.replace(/^\/+/, "")}`,
      source: "local-public"
    };
    if (await isRemoteUrlReachable(localCandidate.url)) return localCandidate;
    logBufferBatch(`URL publica local nao acessivel: ${localCandidate.url}`, "error");
  }

  if (config.bufferMediaSource === "local") {
    return null;
  }

  if (await isRemoteUrlReachable(originalUrl)) return { url: originalUrl, source: "json" };
  return null;
}

let cachedMediaManifest = null;

function mediaManifestByUrl() {
  if (cachedMediaManifest) return cachedMediaManifest;

  cachedMediaManifest = new Map();
  try {
    const manifestPath = resolve(ROOT_DIR, config.outputDir, "media-manifest.json");
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    for (const file of Array.isArray(manifest.files) ? manifest.files : []) {
      if (file.url && file.path) cachedMediaManifest.set(file.url, {
        path: file.path,
        fileName: file.fileName || basename(file.path)
      });
    }
  } catch {
    // Manifesto e opcional. Sem ele, seguimos com a URL original do JSON.
  }

  return cachedMediaManifest;
}

async function isRemoteUrlReachable(url) {
  try {
    const response = await fetch(url, {
      method: "HEAD",
      signal: AbortSignal.timeout(12000),
      headers: {
        "User-Agent": "Mozilla/5.0 BufferMediaValidator/1.0"
      }
    });
    if (response.ok) return true;
    if ([403, 405].includes(response.status)) return await isRemoteUrlReachableWithGet(url);
    return false;
  } catch {
    return await isRemoteUrlReachableWithGet(url);
  }
}

async function isRemoteUrlReachableWithGet(url) {
  try {
    const response = await fetch(url, {
      method: "GET",
      signal: AbortSignal.timeout(12000),
      headers: {
        "Range": "bytes=0-0",
        "User-Agent": "Mozilla/5.0 BufferMediaValidator/1.0"
      }
    });
    return response.ok || response.status === 206;
  } catch {
    return false;
  }
}

function normalizeDatabaseMedia(media) {
  if (Array.isArray(media)) return media;
  if (typeof media === "string") {
    try {
      return JSON.parse(media);
    } catch {
      return [];
    }
  }
  return [];
}

function scheduledDateForIndex(index, { startDate, postsPerDay, times }) {
  const cleanTimes = (Array.isArray(times) && times.length ? times : config.bufferScheduleTimes)
    .map(normalizeTime)
    .filter(Boolean);
  if (!cleanTimes.length) throw badRequest("Configure ao menos um horario em BUFFER_SCHEDULE_TIMES.");

  const effectivePostsPerDay = Math.max(1, postsPerDay || cleanTimes.length);
  const start = startDate ? dateOnly(startDate) : tomorrowDateOnly();
  const dayOffset = Math.floor(index / effectivePostsPerDay);
  const slot = index % effectivePostsPerDay;
  const time = cleanTimes[slot % cleanTimes.length];
  const dueDate = new Date(`${start}T${time}:00`);
  dueDate.setDate(dueDate.getDate() + dayOffset);
  return dueDate.toISOString();
}

function normalizeTime(value) {
  const match = String(value || "").trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) return null;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function dateOnly(value) {
  const date = new Date(`${String(value).slice(0, 10)}T00:00:00`);
  if (Number.isNaN(date.getTime())) throw badRequest("startDate invalido. Use YYYY-MM-DD.");
  return [
    date.getFullYear(),
    String(date.getMonth() + 1).padStart(2, "0"),
    String(date.getDate()).padStart(2, "0")
  ].join("-");
}

function tomorrowDateOnly() {
  const date = new Date();
  date.setDate(date.getDate() + 1);
  return [
    date.getFullYear(),
    String(date.getMonth() + 1).padStart(2, "0"),
    String(date.getDate()).padStart(2, "0")
  ].join("-");
}

function optionalPositiveInteger(value) {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  return Number.isInteger(number) && number > 0 ? number : null;
}

function logBufferBatch(message, level = "log") {
  const line = `[buffer-batch ${new Date().toISOString()}] ${message}`;
  if (level === "error") console.error(line);
  else console.log(line);
}

async function bufferGraphql(query, variables) {
  const response = await fetch(config.bufferApiUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${config.bufferApiKey}`
    },
    body: JSON.stringify({ query, variables })
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const details = bufferErrorMessage(data) || `Buffer respondeu HTTP ${response.status}`;
    const error = new Error(details);
    error.statusCode = response.status;
    error.response = data;
    throw error;
  }
  return data;
}

function bufferErrorMessage(data) {
  if (!data || typeof data !== "object") return "";
  if (data.error) return String(data.error);
  if (data.message) return String(data.message);
  if (Array.isArray(data.errors) && data.errors.length) {
    return data.errors.map((error) => error.message || JSON.stringify(error)).join("; ");
  }
  return "";
}

async function downloadFileWithRetry(url, filePath, { attempts = 3 } = {}) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      if (attempt > 1) logExport(`Tentativa ${attempt}/${attempts}: ${url}`);
      await downloadFile(url, filePath);
      return;
    } catch (error) {
      lastError = error;
      await rm(`${filePath}.download`, { force: true }).catch(() => {});
      if (attempt < attempts) await sleep(900 * attempt);
    }
  }

  throw lastError;
}

async function downloadFile(url, filePath) {
  const tempPath = `${filePath}.download`;
  const response = await fetch(url, {
    signal: AbortSignal.timeout(45000),
    headers: {
      "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,video/*,*/*;q=0.8",
      "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
      "Referer": "https://www.instagram.com/",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    }
  });

  if (!response.ok || !response.body) {
    throw new Error(`HTTP ${response.status}`);
  }

  await pipeline(Readable.fromWeb(response.body), createWriteStream(tempPath));
  await rename(tempPath, filePath);
}

async function createZipFromDirectory(sourceDir, zipPath) {
  const entries = await listFiles(sourceDir);
  const output = createWriteStream(zipPath);
  const centralDirectory = [];
  let offset = 0;

  for (const filePath of entries) {
    const name = toPosixPath(filePath.slice(sourceDir.length + 1));
    const content = await readFile(filePath);
    const crc = crc32(content);
    const nameBuffer = Buffer.from(name);
    const localHeader = zipLocalHeader(nameBuffer, content, crc);

    output.write(localHeader);
    output.write(content);
    centralDirectory.push(zipCentralDirectoryHeader(nameBuffer, content, crc, offset));
    offset += localHeader.length + content.length;
  }

  const centralStart = offset;
  let centralSize = 0;
  for (const header of centralDirectory) {
    output.write(header);
    centralSize += header.length;
  }
  output.write(zipEndRecord(centralDirectory.length, centralSize, centralStart));

  await new Promise((resolvePromise, reject) => {
    output.end(resolvePromise);
    output.on("error", reject);
  });
}

function zipLocalHeader(nameBuffer, content, crc) {
  const header = Buffer.alloc(30);
  header.writeUInt32LE(0x04034b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(0, 6);
  header.writeUInt16LE(0, 8);
  header.writeUInt16LE(0, 10);
  header.writeUInt16LE(0, 12);
  header.writeUInt32LE(crc, 14);
  header.writeUInt32LE(content.length, 18);
  header.writeUInt32LE(content.length, 22);
  header.writeUInt16LE(nameBuffer.length, 26);
  header.writeUInt16LE(0, 28);
  return Buffer.concat([header, nameBuffer]);
}

function zipCentralDirectoryHeader(nameBuffer, content, crc, offset) {
  const header = Buffer.alloc(46);
  header.writeUInt32LE(0x02014b50, 0);
  header.writeUInt16LE(20, 4);
  header.writeUInt16LE(20, 6);
  header.writeUInt16LE(0, 8);
  header.writeUInt16LE(0, 10);
  header.writeUInt16LE(0, 12);
  header.writeUInt16LE(0, 14);
  header.writeUInt32LE(crc, 16);
  header.writeUInt32LE(content.length, 20);
  header.writeUInt32LE(content.length, 24);
  header.writeUInt16LE(nameBuffer.length, 28);
  header.writeUInt16LE(0, 30);
  header.writeUInt16LE(0, 32);
  header.writeUInt16LE(0, 34);
  header.writeUInt16LE(0, 36);
  header.writeUInt32LE(0, 38);
  header.writeUInt32LE(offset, 42);
  return Buffer.concat([header, nameBuffer]);
}

function zipEndRecord(entries, centralSize, centralStart) {
  const header = Buffer.alloc(22);
  header.writeUInt32LE(0x06054b50, 0);
  header.writeUInt16LE(0, 4);
  header.writeUInt16LE(0, 6);
  header.writeUInt16LE(entries, 8);
  header.writeUInt16LE(entries, 10);
  header.writeUInt32LE(centralSize, 12);
  header.writeUInt32LE(centralStart, 16);
  header.writeUInt16LE(0, 20);
  return header;
}

function crc32(buffer) {
  let crc = -1;
  for (const byte of buffer) {
    crc = (crc >>> 8) ^ crcTable[(crc ^ byte) & 0xff];
  }
  return (crc ^ -1) >>> 0;
}

const crcTable = Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = value & 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  return value >>> 0;
});

async function listFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...await listFiles(fullPath));
    else if (entry.isFile()) files.push(fullPath);
  }
  return files;
}

function loadEnv(path) {
  try {
    const content = readFileSync(path, "utf8");
    for (const line of content.split(/\r?\n/)) {
      const match = line.match(/^\s*([\w.-]+)\s*=\s*(.*)\s*$/);
      if (!match || match[1].startsWith("#")) continue;
      const [, key, rawValue] = match;
      if (process.env[key]) continue;
      process.env[key] = rawValue.replace(/^["']|["']$/g, "");
    }
  } catch {
    // .env e opcional.
  }
}

async function serveFrontend(request, response, url) {
  const requestedPath = url.pathname === "/" ? "/index.html" : decodeURIComponent(url.pathname);
  const filePath = resolve(FRONTEND_DIR, `.${requestedPath}`);
  if (!filePath.startsWith(FRONTEND_DIR)) return sendJson(response, 403, { error: "Acesso negado" });

  try {
    await stat(filePath);
    return sendFile(response, filePath);
  } catch {
    return sendFile(response, resolve(FRONTEND_DIR, "index.html"));
  }
}

async function sendFile(response, filePath, downloadName = "") {
  try {
    const fileStat = await stat(filePath);
    response.writeHead(200, {
      "Content-Type": mediaMimeTypes.get(extname(filePath).toLowerCase()) || "application/octet-stream",
      "Content-Length": fileStat.size,
      ...(downloadName ? { "Content-Disposition": `attachment; filename="${downloadName}"` } : {})
    });
    createReadStream(filePath).pipe(response);
  } catch {
    sendJson(response, 404, { error: "Arquivo nao encontrado" });
  }
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, { "Content-Type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(payload, null, 2));
}

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const content = Buffer.concat(chunks).toString("utf8");
  return content ? JSON.parse(content) : {};
}

function extensionFromUrl(url, kind) {
  try {
    const pathname = new URL(url).pathname;
    const extension = extname(pathname).toLowerCase();
    if (extension) return extension === ".jpeg" ? ".jpg" : extension;
  } catch {
    // Segue para extensao por tipo.
  }
  return kind === "video" ? ".mp4" : ".jpg";
}

function inferKind(value = "", fallback = "image") {
  const text = String(value).toLowerCase();
  if (text.includes("video") || [".mp4", ".mov", ".webm"].some((extension) => text.includes(extension))) return "video";
  return String(fallback).toLowerCase() === "video" ? "video" : "image";
}

function sanitizeFileName(value) {
  return value.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function toPosixPath(value) {
  return value.replaceAll("\\", "/");
}

function sleep(ms) {
  return new Promise((resolvePromise) => setTimeout(resolvePromise, ms));
}

async function exists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function graphqlString(value) {
  return JSON.stringify(String(value));
}

function required(value, fieldName) {
  if (!value) throw badRequest(`Campo obrigatorio: ${fieldName}.`);
  return value;
}

function ensureBufferCredentials() {
  if (!config.bufferApiKey) throw badRequest("Configure BUFFER_API_KEY no .env.");
}

function ensureDatabase() {
  if (!database) throw badRequest("Configure DATABASE_URL no .env.");
}

function badRequest(message) {
  const error = new Error(message);
  error.statusCode = 400;
  return error;
}
