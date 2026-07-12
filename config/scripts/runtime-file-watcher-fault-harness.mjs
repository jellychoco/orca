import { createRequire } from 'node:module'
import { existsSync } from 'node:fs'
import { mkdtemp, realpath, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { build } from 'esbuild'

const ENTRY_PATH = resolve('out/main/parcel-watcher-process-entry.js')
const SUPERVISOR_SOURCE = resolve('src/main/ipc/parcel-watcher-process-supervisor.ts')
const WAIT_TIMEOUT_MS = 15_000
const require = createRequire(import.meta.url)

function withTimeout(promise, label) {
  return new Promise((resolvePromise, rejectPromise) => {
    const timer = setTimeout(
      () => rejectPromise(new Error(`Timed out waiting for ${label}`)),
      WAIT_TIMEOUT_MS
    )
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolvePromise(value)
      },
      (error) => {
        clearTimeout(timer)
        rejectPromise(error)
      }
    )
  })
}

function nextMatchingEvent(register, predicate, label) {
  return withTimeout(
    new Promise((resolveEvent) => {
      register((events) => {
        if (events.some(predicate)) {
          resolveEvent(events)
        }
      })
    }),
    label
  )
}

async function loadSupervisor(bundleDir) {
  const outfile = join(bundleDir, 'watcher-supervisor.cjs')
  await build({
    entryPoints: [SUPERVISOR_SOURCE],
    bundle: true,
    platform: 'node',
    format: 'cjs',
    outfile,
    external: ['@parcel/watcher', 'electron'],
    logLevel: 'silent'
  })
  return require(outfile).WatcherProcessSupervisor
}

async function main() {
  if (process.platform === 'win32') {
    console.log('[runtime-file-watcher-fault] SKIP: SIGSEGV oracle is macOS/Linux only')
    return
  }
  if (!existsSync(ENTRY_PATH)) {
    throw new Error(`Missing ${ENTRY_PATH}; run pnpm run build:electron-vite first`)
  }

  const createdRootPath = await mkdtemp(join(tmpdir(), 'orca-runtime-watcher-fault-'))
  const bundleDir = await mkdtemp(join(tmpdir(), 'orca-runtime-watcher-harness-'))
  // Parcel reports canonical event paths on macOS, where tmpdir() may use the
  // /var symlink spelling. Keep the oracle in the same path domain.
  const rootPath = await realpath(createdRootPath)
  const WatcherProcessSupervisor = await loadSupervisor(bundleDir)
  const supervisor = new WatcherProcessSupervisor()
  let subscription
  let eventListener = () => undefined
  try {
    let resolveInterruption
    const interrupted = withTimeout(
      new Promise((resolveWait) => {
        resolveInterruption = resolveWait
      }),
      'automatic watcher resubscription'
    )
    subscription = await supervisor.subscribe(
      rootPath,
      (error, events) => {
        if (error) {
          throw error
        }
        eventListener(events)
      },
      { ignore: ['.git', 'node_modules'] },
      {
        delivery: { includeDirectoryMetadata: true, maxEventsPerBatch: 200 },
        onInterruption: () => resolveInterruption()
      }
    )

    const beforeEvent = nextMatchingEvent(
      (listener) => {
        eventListener = listener
      },
      (event) => event.path === join(rootPath, 'before.txt'),
      'pre-crash watch event'
    )
    await writeFile(join(rootPath, 'before.txt'), 'before')
    await beforeEvent

    const firstChildPid = supervisor.child?.pid
    if (!firstChildPid) {
      throw new Error('Watcher supervisor did not expose a live child')
    }
    process.kill(firstChildPid, 'SIGSEGV')
    await interrupted

    const replacementChildPid = supervisor.child?.pid
    if (!replacementChildPid || replacementChildPid === firstChildPid) {
      throw new Error('Watcher supervisor did not replace the faulted child')
    }
    const afterEvent = nextMatchingEvent(
      (listener) => {
        eventListener = listener
      },
      (event) => event.path === join(rootPath, 'after.txt'),
      'post-crash watch event'
    )
    await writeFile(join(rootPath, 'after.txt'), 'after')
    await afterEvent

    console.log(
      JSON.stringify({
        hostPid: process.pid,
        killedWatcherPid: firstChildPid,
        replacementWatcherPid: replacementChildPid,
        hostSurvived: true,
        automaticResubscribe: true,
        postCrashEventDelivered: true
      })
    )
  } finally {
    await subscription?.unsubscribe()
    supervisor.dispose()
    await Promise.all([
      rm(rootPath, { recursive: true, force: true }),
      rm(bundleDir, { recursive: true, force: true })
    ])
  }
}

await main()
