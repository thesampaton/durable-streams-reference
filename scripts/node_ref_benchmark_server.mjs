import { pathToFileURL } from 'node:url'

const modulePath = process.env.NODE_REF_SERVER_MODULE
if (!modulePath) {
  throw new Error('NODE_REF_SERVER_MODULE is required')
}

const { DurableStreamTestServer } = await import(pathToFileURL(modulePath).href)

const port = Number(process.env.PORT || '4438')
const host = process.env.HOST || '127.0.0.1'
const mode = process.env.MODE || 'memory'
const dataDir = mode === 'file' ? (process.env.DATA_DIR || '/tmp/node-ref-file-store') : undefined

const server = new DurableStreamTestServer({ port, host, dataDir })
await server.start()

const shutdown = async () => {
  await server.stop()
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

setInterval(() => {}, 1 << 30)
