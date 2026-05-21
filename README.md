# Buffer Account Manager

Monorepo simples para exportar as midias do JSON do Instagram e criar/consultar posts no Buffer.

## Estrutura

- `backend/`: API Node.js sem dependencias externas.
- `frontend/`: interface estatica servida pelo backend.
- `media-export/`: criada automaticamente na raiz com os arquivos baixados e o `.zip`.

## Configuracao

Crie um arquivo `.env` na raiz usando `.env.example` como base:

```env
PORT=3333
DATABASE_URL=postgresql://USER:PASSWORD@HOST/neondb?sslmode=require&channel_binding=require
BUFFER_API_URL=https://api.buffer.com
BUFFER_API_KEY=YOUR_API_KEY
BUFFER_ORGANIZATION_ID=some_organization_id
BUFFER_CHANNEL_ID=some_channel_id
BUFFER_POST_TYPE=post
BUFFER_REQUEST_DELAY_MS=750
BUFFER_SCHEDULE_TIMES=09:00,13:00,18:00
MEDIA_DATASET_FILE=dataset_instagram-scraper_2026-05-21_15-30-00-859.json
MEDIA_OUTPUT_DIR=media-export
PUBLIC_MEDIA_BASE_URL=https://example.com/media-export
BUFFER_MEDIA_SOURCE=auto
```

`PUBLIC_MEDIA_BASE_URL` e necessario para postar midias antigas do Instagram com seguranca. URLs `scontent...cdninstagram.com` do JSON podem expirar; nesse caso o Buffer nao consegue baixar o arquivo. Com `BUFFER_MEDIA_SOURCE=auto`, o backend prefere URLs publicas dos arquivos em `media-export` quando o manifesto existe, e usa a URL original do JSON apenas como fallback.

Ao iniciar, se `DATABASE_URL` estiver configurado, o backend cria automaticamente as tabelas:

- `instagram_posts`: titulo, descricao/caption, tags, mencoes, dados do post e metadados.
- `instagram_post_media`: URLs de imagens/videos/midias do post, sem baixar ou salvar arquivos no banco.
- `buffer_post_exports`: controle de envio/agendamento no Buffer para evitar duplicar posts em cliques futuros.

## Rodar

```bash
npm run dev
```

Acesse `http://localhost:3333`.

## APIs

### 1. Exportar midias e gerar ZIP

```http
POST /api/media/export
Content-Type: application/json

{
  "force": false
}
```

Baixa `images`, `displayUrl` e campos comuns de video (`videoUrl`, `video_url`, `video`, `videoPlayUrl`) dos posts e `childPosts`, salva em `media-export/files` e gera `media-export/media-export.zip`.

Download do ZIP:

```http
GET /api/media/zip
```

### 2. Criar post no Buffer

Post com imagem:

```http
POST /api/buffer/posts
Content-Type: application/json

{
  "text": "Hello there",
  "channelId": "some_channel_id",
  "mode": "addToQueue",
  "assets": [
    { "type": "image", "url": "https://example.com/image.jpg" }
  ]
}
```

Post agendado:

```json
{
  "text": "Post agendado",
  "mode": "customScheduled",
  "dueAt": "2026-03-26T10:28:47.545Z"
}
```

Post com video:

```json
{
  "text": "Video post",
  "assets": [
    { "type": "video", "url": "https://example.com/video.mp4" }
  ]
}
```

### 3. Consultar posts paginados

```http
GET /api/buffer/posts?status=sent&first=20&after=id_to_start_after
```

Parametros opcionais:

- `status`: `sent`, `scheduled`, `draft`, ou lista separada por virgula.
- `first`: quantidade por pagina, de 1 a 100.
- `after`: cursor da pagina anterior.
- `channelId`: sobrescreve `BUFFER_CHANNEL_ID`.
- `organizationId`: sobrescreve `BUFFER_ORGANIZATION_ID`.

### 4. Exportar JSON para Postgres

```http
POST /api/database/import
Content-Type: application/json

{}
```

Le o JSON da raiz, normaliza os dados necessarios para repostagem e salva no Neon/Postgres. A importacao e idempotente: posts sao atualizados por `id`, e as midias desses posts sao recriadas a partir do JSON.

Status do banco:

```http
GET /api/database/status
```

### 5. Publicar todos os posts do banco no Buffer

Envia todos os posts ainda nao enviados para a fila do Buffer (`mode: addToQueue`) usando texto e assets vindos do banco.

```http
POST /api/buffer/publish-all
Content-Type: application/json

{
  "channelId": "some_channel_id",
  "force": false,
  "limit": 10
}
```

`limit` e opcional e util para testar com poucos posts antes de disparar tudo.

### 6. Agendar todos os posts, 3 por dia

Agenda posts no Buffer com `mode: customScheduled` e `dueAt`. Por padrao usa os horarios de `BUFFER_SCHEDULE_TIMES`.

```http
POST /api/buffer/schedule-all
Content-Type: application/json

{
  "channelId": "some_channel_id",
  "startDate": "2026-05-22",
  "postsPerDay": 3,
  "force": false
}
```

Se `startDate` nao for enviado, o backend comeca no dia seguinte ao dia atual.

### 7. Consultar canais do Buffer

```http
GET /api/buffer/channels
```

Usa `BUFFER_ORGANIZATION_ID` para buscar canais disponiveis no Buffer.
